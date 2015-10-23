package dagger

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"

	"github.com/nsaje/dagger/structs"
)

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
	computationID     string
	buffer            chan *structs.Tuple
	dispatcher        TupleProcessor
	stopCh            chan struct{}
	sentTracker       SentTracker
	lwmTracker        LwmTracker
	heartbeatExtender chan *structs.Tuple
	wg                sync.WaitGroup
}

// StartBufferedDispatcher creates a new buffered dispatcher and starts workers
// that will be consuming off the queue an sending tuples
func StartBufferedDispatcher(compID string, dispatcher TupleProcessor, sentTracker SentTracker, lwmTracker LwmTracker,
	stopCh chan struct{}) *BufferedDispatcher {
	bd := &BufferedDispatcher{
		computationID:     compID,
		buffer:            make(chan *structs.Tuple, 100), // FIXME: make it configurable
		dispatcher:        dispatcher,
		sentTracker:       sentTracker,
		lwmTracker:        lwmTracker,
		stopCh:            stopCh,
		heartbeatExtender: make(chan *structs.Tuple),
	}

	go func() {
		var lastSent *structs.Tuple
		heartbeatTimer := time.NewTimer(time.Second)
		for {
			select {
			case t := <-bd.heartbeatExtender:
				if lastSent == nil || t.Timestamp.After(lastSent.Timestamp) {
					lastSent = t
				}
			case <-heartbeatTimer.C:
				// resend last tuple in case we havent sent anything in a while
				// to update receiver's LWM
				if lastSent != nil {
					lwm, _ := bd.lwmTracker.GetLocalLWM()
					if lwm == maxTime {
						lastSent.LWM = lastSent.Timestamp.Add(time.Nanosecond)
						log.Println("SENDING HEARTBEAT")
						bd.dispatcher.ProcessTuple(lastSent)
					}
				}
			}
			heartbeatTimer.Reset(time.Second)
		}
	}()

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

func (bd *BufferedDispatcher) dispatch() {
	defer bd.wg.Done()
	for {
		select {
		case t, ok := <-bd.buffer:
			if !ok { // channel closed, no more tuples coming in
				return
			}
			lwm, _ := bd.lwmTracker.GetCombinedLWM()
			t.LWM = lwm
			if lwm.After(t.Timestamp) {
				t.LWM = t.Timestamp
			}
			for {
				err := bd.dispatcher.ProcessTuple(t)
				if err == nil {
					if bd.sentTracker != nil {
						bd.sentTracker.SentSuccessfuly(bd.computationID, t)
					}
					bd.heartbeatExtender <- t // notify the LWM heartbeat mechanism that we've sent a tuple
					break
				}
				log.Println("RETRYING")
				time.Sleep(time.Second) // exponential backoff?
			}
		}
	}
}

type subscriberHandler struct {
	client *rpc.Client
}

func newSubscriberHandler(subscriber string) (*subscriberHandler, error) {
	conn, err := net.Dial("tcp", subscriber)
	if err != nil {
		return nil, err
	}
	client := jsonrpc.NewClient(conn)
	return &subscriberHandler{client}, nil
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
