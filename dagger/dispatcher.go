package dagger

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	"github.com/nsaje/dagger/structs"
)

// Dispatcher dispatches tuples to registered subscribers
type Dispatcher struct {
	conf        *Config
	coordinator Coordinator
	connections map[string]*subscriberHandler
}

// NewDispatcher creates a new dispatcher
func NewDispatcher(conf *Config, coordinator Coordinator) *Dispatcher {
	return &Dispatcher{conf, coordinator, make(map[string]*subscriberHandler)}
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
		subHandler, exists := d.connections[s]
		// If a subscriber connection handler doesn't exist, create it
		if !exists {
			subHandler, err = newSubscriberHandler(s)
			if err != nil {
				return err
			}
			d.connections[s] = subHandler
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
}

// StartBufferedDispatcher creates a new buffered dispatcher and starts workers
// that will be consuming off the queue an sending tuples
func StartBufferedDispatcher(compID string, dispatcher TupleProcessor, sentTracker SentTracker,
	stopCh chan struct{}) *BufferedDispatcher {
	bd := &BufferedDispatcher{
		computationID: compID,
		buffer:        make(chan *structs.Tuple, 100), // FIXME: make it configurable
		dispatcher:    dispatcher,
		sentTracker:   sentTracker,
		stopCh:        stopCh,
	}

	// FIXME: configurable number?
	for i := 0; i < 10; i++ {
		go bd.dispatch()
	}

	return bd
}

// ProcessTuple sends the tuple to the buffered channel
func (bd *BufferedDispatcher) ProcessTuple(t *structs.Tuple) error {
	bd.buffer <- t
	return nil
}

func (bd *BufferedDispatcher) dispatch() {
	for {
		select {
		case <-bd.stopCh:
			return
		case t := <-bd.buffer:
			for {
				err := bd.dispatcher.ProcessTuple(t)
				if err == nil {
					bd.sentTracker.SentSuccessfuly(bd.computationID, t)
					break
				}
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
START:
	var reply string
	err := s.client.Call("Receiver.SubmitTuple", t, &reply)
	if err != nil {
		log.Printf("[dispatcher][WARNING] tuple %v failed delivery: %v", t, err)
		goto START
	}
	log.Printf("[dispatcher] ACK received for tuple %s", t)
	return err
}
