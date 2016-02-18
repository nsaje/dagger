package main

import (
	log "github.com/Sirupsen/logrus"
	"time"

	"github.com/nsaje/dagger/producers"
	"github.com/nsaje/dagger/dagger"
)

// TestProducerPlugin produces an incremented value every second
type TestProducerPlugin struct {
	producers.Producer
}

func main() {
	//log.SetPrefix("[testProducer log] ")
	log.Printf("testProducer started")
	p := TestProducerPlugin{producers.InitProducer()}
	counter := 0
	for {
		time.Sleep(1000 * time.Millisecond)
		s := dagger.Record{StreamID: "test", Data: counter, LWM: dagger.Timestamp(time.Now().UnixNano()), Timestamp: dagger.Timestamp(time.Now().UnixNano())}
		p.Stream <- s
		counter++
	}
}
