package producers

import (
	"bitbucket.org/nsaje/dagger/structs"
	"time"
)

// TestProducerPlugin produces an incremented value every second
type TestProducerPlugin struct{}

// StartProducing provides a channel of streams of new produced values
func (p *TestProducerPlugin) StartProducing() <-chan Stream {
	streams := make(chan Stream)

	go func() {
		stream := make(Stream)
		streams <- stream
		counter := 0
		for {
			time.Sleep(1000 * time.Millisecond)
			stream <- structs.Tuple{Timestamp: time.Now(), Data: counter}
			counter++
		}
	}()

	return streams
}
