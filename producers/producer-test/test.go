package main

import (
	"log"
	"time"

	"github.com/nsaje/dagger/producers"
	"github.com/nsaje/dagger/structs"
)

// TestProducerPlugin produces an incremented value every second
type TestProducerPlugin struct {
	producers.Producer
}

func main() {
	log.SetPrefix("[testProducer log] ")
	log.Printf("testProducer started")
	p := TestProducerPlugin{producers.InitProducer()}
	counter := 0
	for {
		time.Sleep(1000 * time.Millisecond)
		s := structs.Tuple{StreamID: "test", Data: counter}
		p.Stream <- s
		counter++
	}
}
