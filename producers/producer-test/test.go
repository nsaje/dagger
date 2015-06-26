package main

import (
	"encoding/json"
	"log"
	"time"

	"bitbucket.org/nsaje/dagger/producers"
	"bitbucket.org/nsaje/dagger/structs"
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
		s, _ := json.Marshal(structs.Tuple{Timestamp: time.Now(), Data: counter})
		p.Stream <- string(s)
		counter++
	}
}
