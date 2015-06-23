package producers

import "time"

// Init initializes the producer
func (p *ProducerPlugin) Init() {
	p.Streams = make(map[string]chan Tuple)
	p.Streams["test stream"] = make(chan Tuple)
}

// Produce produces new tuples into the Stream channel
func (p *ProducerPlugin) Produce() {
	counter := 0
	for {
		time.Sleep(1000 * time.Millisecond)
		p.Streams["test stream"] <- Tuple{time.Now(), counter}
		counter++
	}
}
