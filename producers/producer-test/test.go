package main

import (
	"log"
	"time"

	"github.com/nsaje/dagger/producers"
	"github.com/nsaje/dagger/s"
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
		s := s.Tuple{StreamID: "test", Data: counter, LWM: time.Now(), Timestamp: time.Now()}
		p.Stream <- s
		counter++
	}
}
