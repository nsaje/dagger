package dagger

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"
)

// StreamDispatcher dispatches records from a single stream to registered subscribers
type StreamDispatcher struct {
	streamID     StreamID
	persister    Persister
	lwmTracker   LWMTracker
	coordinator  Coordinator
	groupHandler GroupHandler
	iterators    map[string]*StreamIterator
	notifyCh     chan *Record
	stopCh       chan struct{}
	new          chan string
	dropped      chan string
	last         *Record
}

func NewStreamDispatcher(streamID StreamID, coordinator Coordinator,
	persister Persister, lwmTracker LWMTracker, groupHandler GroupHandler) *StreamDispatcher {
	stopCh := make(chan struct{})
	new, dropped, _ := coordinator.WatchSubscribers(streamID, stopCh) // FIXME: handle errc
	return &StreamDispatcher{
		streamID:     streamID,
		persister:    persister,
		lwmTracker:   lwmTracker,
		coordinator:  coordinator,
		groupHandler: groupHandler,
		iterators:    make(map[string]*StreamIterator),
		notifyCh:     make(chan *Record),
		stopCh:       stopCh,
		new:          new,
		dropped:      dropped,
	}
}

func (sd *StreamDispatcher) ProcessRecord(t *Record) error {
	sd.lwmTracker.BeforeDispatching([]*Record{t})
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
				iter.ProcessRecord(r)
			}
		case subscriber := <-sd.new:
			log.Println("[streamDispatcher] adding subscriber", subscriber)
			from, err := sd.coordinator.GetSubscriberPosition(sd.streamID, subscriber)
			if err != nil {
				log.Println("ERROR", err)
				// FIXME
			}
			subscriberHandler, err := newSubscriberHandler(subscriber)
			if err != nil {
				log.Println("ERROR", err)
				// FIXME
			}
			stopCh := make(chan struct{})
			posUpdates, posErr := sd.coordinator.WatchSubscriberPosition(sd.streamID, subscriber, stopCh)
			iter := &StreamIterator{
				sd.streamID,
				sd.lwmTracker,
				sd.groupHandler,
				posUpdates,
				posErr,
				make(chan *Record),
				stopCh,
				sd.persister,
				subscriberHandler,
			}
			sd.iterators[subscriber] = iter
			log.Println("STARTING DISPATCH FROM", from)
			go iter.Dispatch(from)
			// notify the new iterator of how far we've gotten
			if sd.last != nil {
				iter.ProcessRecord(sd.last)
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
	compID            StreamID
	lwmTracker        LWMTracker
	groupHandler      GroupHandler
	posUpdates        chan Timestamp
	posErr            chan error
	notify            chan *Record
	stopCh            chan struct{}
	persister         Persister
	subscriberHandler *subscriberHandler
}

func (si *StreamIterator) ProcessRecord(t *Record) error {
	log.Println("[iterator] processing", t)
	si.notify <- t
	return nil
}

func (si *StreamIterator) Stop() {
	close(si.stopCh)
}

func (si *StreamIterator) Dispatch(startAt Timestamp) {
	fromCh := make(chan Timestamp)
	toCh := make(chan Timestamp)
	toSend := make(chan *Record)
	sentSuccessfuly := make(chan *Record)
	errc := make(chan error)

	var lastSent *Record
	flushAfter := 500 * time.Millisecond
	lwmFlushTimer := time.NewTimer(flushAfter)
	lwmFlushTimer.Stop()

	go MovingLimitRead(si.persister, si.compID, "p", fromCh, toCh, toSend, errc)
	fromCh <- startAt
	// toCh <- Timestamp(1<<62 - 1)

	for {
		select {
		case <-si.stopCh:
			return
		case updatedPos := <-si.posUpdates:
			log.Println("[iterator] updating position to:", updatedPos)
			if si.groupHandler != nil {
				areWeLeader, _, _ := si.groupHandler.GetStatus()
				if !areWeLeader {
					fromCh <- updatedPos
				}
			}
		case <-si.posErr:
			log.Println("[ERROR] position watcher error")
			// close(si.stopCh) // FIXME: do this or not?
		case r := <-toSend:
			log.Println("[iterator] sending record:", r)
			compLWM := si.lwmTracker.GetCombinedLWM()
			if compLWM < r.Timestamp {
				r.LWM = compLWM
			} else {
				r.LWM = r.Timestamp
			}
			si.subscriberHandler.ProcessRecordAsync(r, sentSuccessfuly)
		case <-lwmFlushTimer.C:
			log.Println("[iterator] flush timer firing")
			if lastSent != nil {
				lastSent.LWM = lastSent.Timestamp + 1
				err := si.subscriberHandler.ProcessRecord(lastSent)
				if err != nil {
					log.Println("ERROR:", err)
				}
			}
		case r := <-sentSuccessfuly:
			lastSent = r
			lwmFlushTimer.Reset(flushAfter)
			si.lwmTracker.SentSuccessfuly("FIXME", r)
			log.Println("[iterator] newFrom updated:", r, r.Timestamp+1)
		case r := <-si.notify:
			toCh <- r.Timestamp
			log.Println("[iterator] newTo updated:", r)
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

// ProcessRecord sends record to registered subscribers via RPC
func (d *Dispatcher) ProcessRecord(t *Record) error {
	subscribers, err := d.coordinator.GetSubscribers(t.StreamID)
	log.Printf("[dispatcher] record: %v, subscribers: %v\n", t, subscribers)
	if err != nil {
		return err
	}
	subscriberHandlers := make([]RecordProcessor, len(subscribers))
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
	streamID    StreamID
	buffer      chan *Record
	dispatcher  RecordProcessor
	stopCh      chan struct{}
	sentTracker SentTracker
	lwmTracker  LWMTracker
	lwmFlush    chan *Record
	wg          sync.WaitGroup
}

// StartBufferedDispatcher creates a new buffered dispatcher and starts workers
// that will be consuming off the queue an sending records
func StartBufferedDispatcher(compID StreamID, dispatcher RecordProcessor, sentTracker SentTracker, lwmTracker LWMTracker,
	stopCh chan struct{}) *BufferedDispatcher {
	bd := &BufferedDispatcher{
		streamID:    compID,
		buffer:      make(chan *Record, 100), // FIXME: make it configurable
		dispatcher:  dispatcher,
		sentTracker: sentTracker,
		lwmTracker:  lwmTracker,
		stopCh:      stopCh,
		lwmFlush:    make(chan *Record),
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

// ProcessRecord sends the record to the buffered channel
func (bd *BufferedDispatcher) ProcessRecord(t *Record) error {
	bd.lwmTracker.BeforeDispatching([]*Record{t})
	bd.buffer <- t
	return nil
}

func (bd *BufferedDispatcher) lwmFlusher() {
	var lastSent *Record
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
						err := bd.dispatcher.ProcessRecord(lastSent)
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
				err := bd.dispatcher.ProcessRecord(t)
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

func (s *subscriberHandler) ProcessRecord(t *Record) error {
	var reply string
	err := s.client.Call("Receiver.SubmitRecord", t, &reply)
	if err != nil {
		log.Printf("[dispatcher][WARNING] record %v failed delivery: %v", t, err)
		return err
	}
	log.Printf("[dispatcher] ACK received for record %s", t)
	return nil
}

func (s *subscriberHandler) ProcessRecordAsync(t *Record, sentSuccessfuly chan *Record) {
	var reply string
	s.semaphore <- struct{}{}
	call := s.client.Go("Receiver.SubmitRecord", t, &reply, nil)
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
