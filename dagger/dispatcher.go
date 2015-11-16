package dagger

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"

	"github.com/nsaje/dagger/s"
)

// StreamDispatcher dispatches records from a single stream to registered subscribers
type StreamDispatcher struct {
	streamID     s.StreamID
	persister    Persister
	lwmTracker   LWMTracker
	coordinator  Coordinator
	groupHandler GroupHandler
	iterators    map[string]*StreamIterator
	notifyCh     chan *s.Record
	stopCh       chan struct{}
	new          chan NewSubscriber
	dropped      chan string
	last         *s.Record
}

func NewStreamDispatcher(streamID s.StreamID, coordinator Coordinator,
	persister Persister, lwmTracker LWMTracker, groupHandler GroupHandler) *StreamDispatcher {
	stopCh := make(chan struct{})
	new, dropped := coordinator.WatchSubscribers(streamID, stopCh)
	return &StreamDispatcher{
		streamID:     streamID,
		persister:    persister,
		lwmTracker:   lwmTracker,
		coordinator:  coordinator,
		groupHandler: groupHandler,
		iterators:    make(map[string]*StreamIterator),
		notifyCh:     make(chan *s.Record),
		stopCh:       stopCh,
		new:          new,
		dropped:      dropped,
	}
}

func (sd *StreamDispatcher) ProcessTuple(t *s.Record) error {
	sd.lwmTracker.BeforeDispatching([]*s.Record{t})
	sd.notifyCh <- t
	return nil
}

func (sd *StreamDispatcher) Run() {
	log.Println("RUNNING DISPATCHER")
	for {
		select {
		case <-sd.stopCh:
			return
		case r := <-sd.notifyCh:
			log.Println("[streamDispatcher] notifying iterators")
			sd.last = r
			for _, iter := range sd.iterators {
				iter.ProcessTuple(r)
			}
		case subscriber := <-sd.new:
			log.Println("[streamDispatcher] adding subscriber", subscriber)
			subscriberHandler, err := newSubscriberHandler(subscriber.Addr)
			if err != nil {

			}
			stopCh := make(chan struct{})
			posUpdates := make(chan s.Timestamp)
			go sd.coordinator.WatchSubscriberPosition(sd.streamID, subscriber.Addr, stopCh, posUpdates)
			iter := &StreamIterator{
				sd.streamID,
				sd.lwmTracker,
				sd.groupHandler,
				posUpdates,
				make(chan *s.Record),
				stopCh,
				sd.persister,
				subscriberHandler,
			}
			sd.iterators[subscriber.Addr] = iter
			log.Println("STARTING DISPATCH FROM", subscriber.From)
			go iter.Dispatch(subscriber.From)
			// notify the new iterator of how far we've gotten
			if sd.last != nil {
				iter.ProcessTuple(sd.last)
			}
		case subscriber := <-sd.dropped:
			log.Println("[streamDispatcher] removing subscriber", subscriber)
			subIterator := sd.iterators[subscriber]
			if subIterator != nil {
				subIterator.Stop()
			}
			delete(sd.iterators, subscriber)
		}
	}
}

type StreamIterator struct {
	compID            s.StreamID
	lwmTracker        LWMTracker
	groupHandler      GroupHandler
	positionUpdates   chan s.Timestamp
	notify            chan *s.Record
	stopCh            chan struct{}
	persister         Persister
	subscriberHandler *subscriberHandler
}

func (si *StreamIterator) ProcessTuple(t *s.Record) error {
	log.Println("[iterator] processing", t)
	si.notify <- t
	return nil
}

func (si *StreamIterator) Stop() {
	close(si.stopCh)
}

func (si *StreamIterator) Dispatch(startAt s.Timestamp) {
	from := startAt
	to := s.Timestamp(1<<63 - 1)
	// var newTo, newFrom s.Timestamp
	// newTo = to
	readCompletedCh := make(chan struct{})
	toSend := make(chan *s.Record)
	var newDataReady, readCompleted bool
	sentSuccessfuly := make(chan *s.Record)

	var lastSent *s.Record
	flushAfter := 500 * time.Millisecond
	lwmFlushTimer := time.NewTimer(flushAfter)
	lwmFlushTimer.Stop()

	// updatePosCh := si.groupHandler.

	startNewRead := func() {
		log.Println("[iterator] reading:", from, to)
		lwmFlushTimer.Stop()
		newDataReady = false
		readCompleted = false
		if si.groupHandler != nil {
			areWeLeader, _, _ := si.groupHandler.GetStatus()
			if !areWeLeader {
				return // don't send if we aren't the leader
			}
		}
		// to + 1 so the 'to' is included
		si.persister.ReadBuffer1(si.compID, "p", from, to+1, toSend, readCompletedCh)
	}
	readCompleted = true
	// startNewRead()

	for {
		select {
		case <-si.stopCh:
			return
		case updatedPos := <-si.positionUpdates:
			log.Println("[iterator] updating position to:", updatedPos)
			// from = updatedPos
		case r := <-toSend:
			log.Println("[iterator] sending record:", r)
			compLWM := si.lwmTracker.GetCombinedLWM()
			if compLWM < r.Timestamp {
				r.LWM = compLWM
			} else {
				r.LWM = r.Timestamp
			}
			si.subscriberHandler.ProcessTupleAsync(r, sentSuccessfuly)
		case <-lwmFlushTimer.C:
			log.Println("[iterator] flush timer firing")
			if lastSent != nil {
				lastSent.LWM = lastSent.Timestamp + 1
				err := si.subscriberHandler.ProcessTuple(lastSent)
				if err != nil {
					log.Println("ERROR:", err)
				}
			}
		case r := <-sentSuccessfuly:
			// + 1 so the just successfuly sent record isn't sent again
			from = r.Timestamp + 1
			lastSent = r
			si.lwmTracker.SentSuccessfuly("FIXME", r)
			log.Println("[iterator] newFrom updated:", r, from)
		case r := <-si.notify:
			to = r.Timestamp
			log.Println("[iterator] newTo updated:", r)
			newDataReady = true
			log.Println("[iterator] newDataReadyCh", newDataReady, readCompleted)
			if newDataReady && readCompleted {
				log.Println("starting new read...")
				// startNewRead <- struct{}{}
				// from = to
				// to = newTo
				startNewRead()
				log.Println("...new read started")
			}
		case <-readCompletedCh:
			log.Println("[iterator] readCompletedCh", newDataReady, readCompleted)
			readCompleted = true
			lwmFlushTimer.Reset(flushAfter)
			if newDataReady && readCompleted {
				log.Println("starting new read...")
				// from = to
				// to = newTo
				startNewRead()
				log.Println("...new read started")
			}
		}
	}
}

// Dispatcher dispatches records to registered subscribers
type Dispatcher struct {
	conf        *Config
	coordinator Coordinator
	connections map[string]*subscriberHandler
	sync.RWMutex
}

// NewDispatcher creates a new dispatcher
func NewDispatcher(conf *Config, coordinator Coordinator) *Dispatcher {
	return &Dispatcher{conf, coordinator, make(map[string]*subscriberHandler),
		sync.RWMutex{}}
}

// ProcessTuple sends record to registered subscribers via RPC
func (d *Dispatcher) ProcessTuple(t *s.Record) error {
	subscribers, err := d.coordinator.GetSubscribers(t.StreamID)
	log.Printf("[dispatcher] record: %v, subscribers: %v\n", t, subscribers)
	if err != nil {
		return err
	}
	subscriberHandlers := make([]TupleProcessor, len(subscribers))
	for i, s := range subscribers {
		d.RLock()
		subHandler, exists := d.connections[s]
		d.RUnlock()
		// If a subscriber connection handler doesn't exist, create it
		if !exists {
			d.Lock()
			_, exists := d.connections[s]
			if !exists {
				subHandler, err = newSubscriberHandler(s)
				if err != nil {
					return err
				}
				d.connections[s] = subHandler
			}
			d.Unlock()
		}
		subscriberHandlers[i] = subHandler
	}
	return ProcessMultipleProcessors(subscriberHandlers, t)
}

// BufferedDispatcher bufferes produced records and sends them with retrying
// until they are ACKed
type BufferedDispatcher struct {
	streamID    s.StreamID
	buffer      chan *s.Record
	dispatcher  TupleProcessor
	stopCh      chan struct{}
	sentTracker SentTracker
	lwmTracker  LWMTracker
	lwmFlush    chan *s.Record
	wg          sync.WaitGroup
}

// StartBufferedDispatcher creates a new buffered dispatcher and starts workers
// that will be consuming off the queue an sending records
func StartBufferedDispatcher(compID s.StreamID, dispatcher TupleProcessor, sentTracker SentTracker, lwmTracker LWMTracker,
	stopCh chan struct{}) *BufferedDispatcher {
	bd := &BufferedDispatcher{
		streamID:    compID,
		buffer:      make(chan *s.Record, 100), // FIXME: make it configurable
		dispatcher:  dispatcher,
		sentTracker: sentTracker,
		lwmTracker:  lwmTracker,
		stopCh:      stopCh,
		lwmFlush:    make(chan *s.Record),
	}

	go bd.lwmFlusher()

	// FIXME: configurable number?
	for i := 0; i < 10; i++ {
		bd.wg.Add(1)
		go bd.dispatch()
	}

	return bd
}

// Stop blocks until all buffered records are sent
func (bd *BufferedDispatcher) Stop() {
	close(bd.buffer)
	bd.wg.Wait()
}

// ProcessTuple sends the record to the buffered channel
func (bd *BufferedDispatcher) ProcessTuple(t *s.Record) error {
	bd.lwmTracker.BeforeDispatching([]*s.Record{t})
	bd.buffer <- t
	return nil
}

func (bd *BufferedDispatcher) lwmFlusher() {
	var lastSent *s.Record
	flushAfter := 500 * time.Millisecond
	lwmFlushTimer := time.NewTimer(flushAfter)
	for {
		select {
		case r := <-bd.lwmFlush:
			if lastSent == nil || r.Timestamp > lastSent.Timestamp {
				lastSent = r
			}
			lwmFlushTimer.Reset(flushAfter)
		case <-lwmFlushTimer.C:
			// resend last record in case we havent sent anything in a while
			// to update receiver's LWM
			if lastSent != nil {
				lwm := bd.lwmTracker.GetLocalLWM()
				if lwm == maxTime {
					lastSent.LWM = lastSent.Timestamp + 1
					log.Println("[dispatcher] flushing LWM on stream", bd.streamID)
					for {
						err := bd.dispatcher.ProcessTuple(lastSent)
						if err != nil {
							continue
						}
						break
					}
				}
			}
		}
	}
}

func (bd *BufferedDispatcher) dispatch() {
	defer bd.wg.Done()
	for {
		select {
		case t, ok := <-bd.buffer:
			if !ok { // channel closed, no more records coming in
				return
			}
			t.LWM = bd.lwmTracker.GetCombinedLWM()
			for {
				err := bd.dispatcher.ProcessTuple(t)
				if err != nil {
					log.Println("[dispatcher] Retrying record", t)
					time.Sleep(time.Second) // exponential backoff?
					continue
				}
				if bd.sentTracker != nil {
					bd.sentTracker.SentSuccessfuly(bd.streamID, t)
				}
				bd.lwmFlush <- t // notify the LWM heartbeat mechanism that we've sent a record
				break
			}
		}
	}
}

type subscriberHandler struct {
	client    *rpc.Client
	semaphore chan struct{}
}

func newSubscriberHandler(subscriber string) (*subscriberHandler, error) {
	conn, err := net.Dial("tcp", subscriber)
	if err != nil {
		return nil, err
	}
	client := jsonrpc.NewClient(conn)
	return &subscriberHandler{client, make(chan struct{}, 10)}, nil // FIXME: make configurable
}

func (s *subscriberHandler) ProcessTuple(t *s.Record) error {
	var reply string
	err := s.client.Call("Receiver.SubmitTuple", t, &reply)
	if err != nil {
		log.Printf("[dispatcher][WARNING] record %v failed delivery: %v", t, err)
		return err
	}
	log.Printf("[dispatcher] ACK received for record %s", t)
	return nil
}

func (s *subscriberHandler) ProcessTupleAsync(t *s.Record, sentSuccessfuly chan *s.Record) {
	var reply string
	s.semaphore <- struct{}{}
	call := s.client.Go("Receiver.SubmitTuple", t, &reply, nil)
	go func() {
		<-call.Done
		<-s.semaphore
		err := call.Error
		if err != nil {
			log.Printf("[dispatcher][WARNING] record %v failed delivery: %v", t, err)
			// FIXME error handling
			// return err
			return
		}
		sentSuccessfuly <- t
		log.Printf("[dispatcher] ACK received for record %s", t)
	}()
}
