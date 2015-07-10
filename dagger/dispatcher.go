package dagger

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

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

// StartDispatching sends tuples to registered subscribers via RPC
func (d *Dispatcher) StartDispatching(output chan *structs.Tuple) {
	log.Println("Starting dispatching")
	for t := range output {
		subscribers, err := d.coordinator.GetSubscribers(t.StreamID)
		log.Printf("[dispatcher] tuple: %v, subscribers: %v\n", t, subscribers)
		if err != nil {
			log.Fatal(err) // FIXME
		}
		for _, s := range subscribers {
			subHandler, exists := d.connections[s]
			if !exists {
				conn, err := net.Dial("tcp", s)
				if err != nil {
					log.Println("failed to dial subscriber: ", err)
					continue // FIXME
				}
				client := jsonrpc.NewClient(conn)
				subHandler = &subscriberHandler{client}
				d.connections[s] = subHandler
			}
			go subHandler.send(t)
		}
	}
	log.Println("Dispatching exiting")
}

type subscriberHandler struct {
	client *rpc.Client
}

func (s *subscriberHandler) send(t *structs.Tuple) {
	var reply string
	for {
		// log.Println("Calling...")
		s.client.Call("Receiver.SubmitTuple", t, &reply)
		log.Println("reply: ", reply)
		if reply == "ok" {
			log.Printf("[dispatcher] send successful to %s of tuple %v", "", t)
			// log.Println("Call succeeded")
			return
		}
		log.Printf("[dispatcher] reply not ok: %s", reply)
		time.Sleep(time.Second)
		log.Printf("[dispatcher] retrying")
	}
}
