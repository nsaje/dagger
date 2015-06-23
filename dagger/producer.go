package dagger

import "time"

// Producer creates tuples from the outside world
type Producer struct {
	Name   string
	Stream chan Tuple
}

// Init initializes the producer
func (p *Producer) Init() {
	p.Stream = make(chan Tuple)
}

// Produce produces new tuples into the Stream channel
func (p *Producer) Produce() {
	counter := 0
	for {
		time.Sleep(1000 * time.Millisecond)
		p.Stream <- 5
	}
}
