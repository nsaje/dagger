package main

import (
	"log"
	"net"
	"net/rpc/jsonrpc"

	"bitbucket.org/nsaje/dagger/structs"
)

// Dispatcher dispatches tuples to registered subscribers
type Dispatcher struct {
	conf        *Config
	coordinator Coordinator
}

func startDispatching(conf *Config, coordinator Coordinator, output chan structs.Tuple) {
	// d := &Dispatcher{conf, coordinator}
	log.Println("Starting dispatching")
	for t := range output {
		log.Printf("Handling tuple: %s\n", t)
		subscribers, err := coordinator.GetSubscribers(t.StreamID)
		log.Printf("Found subscribers: %v\n", subscribers)
		if err != nil {
			die("%v", err)
		}
		for _, s := range subscribers {
			conn, err := net.Dial("tcp", s)
			if err != nil {
				log.Println("failed to dial subscriber: ", err)
				continue
			}
			var reply string
			log.Println("Calling...")
			client := jsonrpc.NewClient(conn)
			client.Call("Receiver.SubmitTuple", t, &reply)
			if reply != "ok" {
				log.Println("Receiver.SubmitTuple reply not ok: %s", reply)
				continue
			}
			log.Println("Call succeeded")
		}
	}
	log.Println("Dispatching exiting")
}
