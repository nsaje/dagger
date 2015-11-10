package dagger

import (
	"log"
	"math"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"

	"github.com/nsaje/dagger/structs"
)

// StreamDispatcher dispatches tuples from a single stream to registered subscribers
type StreamDispatcher struct {
	computationID string
	persister     Persister
	lwmTracker    LWMTracker
	coordinator   Coordinator
	groupHandler  GroupHandler
	iterators     map[string]*StreamIterator
	notifyCh      chan *structs.Tuple
	stopCh        chan struct{}
	new           chan newSubscriber
	dropped       chan string
	last          *structs.Tuple
}

func NewStreamDispatcher(streamID string, coordinator Coordinator,
	persister Persister, lwmTracker LWMTracker, groupHandler GroupHandler) *StreamDispatcher {
	stopCh := make(chan struct{})
	new, dropped := coordinator.WatchSubscribers(streamID, stopCh)
	return &StreamDispatcher{
		computationID: streamID,
		persister:     persister,
		lwmTracker:    lwmTracker,
		coordinator:   coordinator,
		groupHandler:  groupHandler,
		iterators:     make(map[string]*StreamIterator),
		notifyCh:      make(chan *structs.Tuple),
		stopCh:        stopCh,
		new:           new,
		dropped:       dropped,
	}
}

func (sd *StreamDispatcher) ProcessTuple(t *structs.Tuple) error {
	sd.lwmTracker.BeforeDispatching([]*structs.Tuple{t})
	sd.notifyCh <- t
	return nil
}

func (sd *StreamDispatcher) Run() {
	log.Println("RUNNING DISPATCHER")
	for {
		select {
		case <-sd.stopCh:
			return
		case t := <-sd.notifyCh:
			log.Println("[streamDispatcher] notifying iterators")
			sd.last = t
			for _, iter := range sd.iterators {
				iter.ProcessTuple(t)
			}
		case subscriber := <-sd.new:
			log.Println("[streamDispatcher] adding subscriber", subscriber)
			subscriberHandler, err := newSubscriberHandler(subscriber.addr)
			if err != nil {

			}
			stopCh := make(chan struct{})
			posUpdates := make(chan time.Time)
			go sd.coordinator.WatchSubscriberPosition(sd.computationID, subscriber.addr, stopCh, posUpdates)
			iter := &StreamIterator{
				sd.computationID,
				sd.lwmTracker,
				sd.groupHandler,
				posUpdates,
				make(chan *structs.Tuple),
				stopCh,
				sd.persister,
				subscriberHandler,
			}
			sd.iterators[subscriber.addr] = iter
			log.Println("STARTING DISPATCH FROM", subscriber.from)
			go iter.Dispatch(subscriber.from)
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
	compID            string
	lwmTracker        LWMTracker
	groupHandler      GroupHandler
	positionUpdates   chan time.Time
	notify            chan *structs.Tuple
	stopCh            chan struct{}
	persister         Persister
	subscriberHandler *subscriberHandler
}

func (si *StreamIterator) ProcessTuple(t *structs.Tuple) error {
	log.Println("[iterator] processing", t)
	si.notify <- t
	return nil
}

func (si *StreamIterator) Stop() {
	close(si.stopCh)
}

func (si *StreamIterator) Dispatch(startAt time.Time) {
	from := startAt
	to := time.Unix(0, int64(math.Pow(2, 63)-1))
	// var newTo, newFrom time.Time
	// newTo = to
	readCompletedCh := make(chan struct{})
	toSend := make(chan *structs.Tuple)
	var newDataReady, readCompleted bool
	sentSuccessfuly := make(chan *structs.Tuple)

	var lastSent *structs.Tuple
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
		si.persister.ReadBuffer1(si.compID, "p", from, to, toSend, readCompletedCh)
	}
	readCompleted = true
	// startNewRead()

	for {
		select {
		case <-si.stopCh:
			return
		case updatedPos := <-si.positionUpdates:
			log.Println("[iterator] updating position to:", updatedPos)
			from = updatedPos
		case t := <-toSend:
			log.Println("[iterator] sending tuple:", t)
			// t.LWM = t.Timestamp.Add(time.Nanosecond)
			compLWM := si.lwmTracker.GetCombinedLWM()
			if compLWM.Before(t.Timestamp) {
				t.LWM = compLWM
			} else {
				t.LWM = t.Timestamp
			}
			// t.LWM = bd.lwmTracker.GetCombinedLWM()
			si.subscriberHandler.ProcessTupleAsync(t, sentSuccessfuly)
		case <-lwmFlushTimer.C:
			log.Println("[iterator] flush timer firing")
			if lastSent != nil {
				lastSent.LWM = lastSent.Timestamp.Add(time.Nanosecond)
				err := si.subscriberHandler.ProcessTuple(lastSent)
				if err != nil {
					log.Println("ERROR:", err)
				}
			}
		case t := <-sentSuccessfuly:
			from = t.Timestamp
			lastSent = t
			si.lwmTracker.SentSuccessfuly("FIXME", t)
			log.Println("[iterator] newFrom updated:", t, from.UnixNano())
		case t := <-si.notify:
			to = t.Timestamp
			log.Println("[iterator] newTo updated:", t)
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

// Dispatcher dispatches tuples to registered subscribers
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

// ProcessTuple sends tuple to registered subscribers via RPC
func (d *Dispatcher) ProcessTuple(t *structs.Tuple) error {
	subscribers, err := d.coordinator.GetSubscribers(t.StreamID)
	log.Printf("[dispatcher] tuple: %v, subscribers: %v\n", t, subscribers)
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

// BufferedDispatcher bufferes produced tuples and sends them with retrying
// until they are ACKed
type BufferedDispatcher struct {
	computationID string
	buffer        chan *structs.Tuple
	dispatcher    TupleProcessor
	stopCh        chan struct{}
	sentTracker   SentTracker
	lwmTracker    LWMTracker
	lwmFlush      chan *structs.Tuple
	wg            sync.WaitGroup
}

// StartBufferedDispatcher creates a new buffered dispatcher and starts workers
// that will be consuming off the queue an sending tuples
func StartBufferedDispatcher(compID string, dispatcher TupleProcessor, sentTracker SentTracker, lwmTracker LWMTracker,
	stopCh chan struct{}) *BufferedDispatcher {
	bd := &BufferedDispatcher{
		computationID: compID,
		buffer:        make(chan *structs.Tuple, 100), // FIXME: make it configurable
		dispatcher:    dispatcher,
		sentTracker:   sentTracker,
		lwmTracker:    lwmTracker,
		stopCh:        stopCh,
		lwmFlush:      make(chan *structs.Tuple),
	}

	go bd.lwmFlusher()

	// FIXME: configurable number?
	for i := 0; i < 10; i++ {
		bd.wg.Add(1)
		go bd.dispatch()
	}

	return bd
}

// Stop blocks until all buffered tuples are sent
func (bd *BufferedDispatcher) Stop() {
	close(bd.buffer)
	bd.wg.Wait()
}

// ProcessTuple sends the tuple to the buffered channel
func (bd *BufferedDispatcher) ProcessTuple(t *structs.Tuple) error {
	bd.lwmTracker.BeforeDispatching([]*structs.Tuple{t})
	bd.buffer <- t
	return nil
}

func (bd *BufferedDispatcher) lwmFlusher() {
	var lastSent *structs.Tuple
	flushAfter := 500 * time.Millisecond
	lwmFlushTimer := time.NewTimer(flushAfter)
	for {
		select {
		case t := <-bd.lwmFlush:
			if lastSent == nil || t.Timestamp.After(lastSent.Timestamp) {
				lastSent = t
			}
			lwmFlushTimer.Reset(flushAfter)
		case <-lwmFlushTimer.C:
			// resend last tuple in case we havent sent anything in a while
			// to update receiver's LWM
			if lastSent != nil {
				lwm := bd.lwmTracker.GetLocalLWM()
				if lwm == maxTime {
					lastSent.LWM = lastSent.Timestamp.Add(time.Nanosecond)
					log.Println("[dispatcher] flushing LWM on stream", bd.computationID)
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
			if !ok { // channel closed, no more tuples coming in
				return
			}
			t.LWM = bd.lwmTracker.GetCombinedLWM()
			for {
				err := bd.dispatcher.ProcessTuple(t)
				if err != nil {
					log.Println("[dispatcher] Retrying tuple", t)
					time.Sleep(time.Second) // exponential backoff?
					continue
				}
				if bd.sentTracker != nil {
					bd.sentTracker.SentSuccessfuly(bd.computationID, t)
				}
				bd.lwmFlush <- t // notify the LWM heartbeat mechanism that we've sent a tuple
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

func (s *subscriberHandler) ProcessTuple(t *structs.Tuple) error {
	var reply string
	err := s.client.Call("Receiver.SubmitTuple", t, &reply)
	if err != nil {
		log.Printf("[dispatcher][WARNING] tuple %v failed delivery: %v", t, err)
		return err
	}
	log.Printf("[dispatcher] ACK received for tuple %s", t)
	return nil
}

func (s *subscriberHandler) ProcessTupleAsync(t *structs.Tuple, sentSuccessfuly chan *structs.Tuple) {
	var reply string
	s.semaphore <- struct{}{}
	call := s.client.Go("Receiver.SubmitTuple", t, &reply, nil)
	go func() {
		<-call.Done
		<-s.semaphore
		err := call.Error
		if err != nil {
			log.Printf("[dispatcher][WARNING] tuple %v failed delivery: %v", t, err)
			// FIXME error handling
			// return err
			return
		}
		sentSuccessfuly <- t
		log.Printf("[dispatcher] ACK received for tuple %s", t)
	}()
}
