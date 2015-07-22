package dagger

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"

	"bitbucket.org/nsaje/dagger/structs"
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
	return s.client.Call("Receiver.SubmitTuple", t, &reply)
}
