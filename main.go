package main

import (
	"bitbucket.org/nsaje/dagger/producers"
	"fmt"
)

func main() {
	var p producers.TestProducerPlugin
	streams := p.StartProducing()
	for {
		select {
		case stream, ok := <-streams:
			if !ok {
				panic("what?")
			}
			go func(stream producers.Stream) {
				for {
					val := <-stream
					fmt.Printf("%v\n", val)
				}
			}(stream)
		}
	}
}
