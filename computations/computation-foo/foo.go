package main

import (
	"fmt"
	"log"

	"bitbucket.org/nsaje/dagger/computations"
)

// FooComputation simply prepends "fooized" to a tuple
type FooComputation struct {
	*computations.DefaultComputation
	counter int
}

// State returns this computation's state
func (c *FooComputation) State() interface{} {
	return c.counter
}

func main() {
	log.SetPrefix("[fooComputation log] ")
	log.Printf("fooComputation started")
	c := &FooComputation{computations.NewDefaultComputation(), 0}
	computations.StartPlugin(c)
	for t := range c.Input() {
		t.Data = fmt.Sprintf("fooized: %v", t.Data)
		t.StreamID = "testfoo"
		c.Output() <- t
		c.counter++
	}
}
