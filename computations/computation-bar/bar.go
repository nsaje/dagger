package main

import (
	"fmt"
	"log"

	"bitbucket.org/nsaje/dagger/computations"
)

// BarComputation simply prepends "barized" to a tuple
type BarComputation struct {
	*computations.DefaultComputation
	counter int
}

// State returns this computation's state
func (c *BarComputation) State() interface{} {
	return c.counter
}

func main() {
	log.SetPrefix("[barComputation log] ")
	log.Printf("barComputation started")
	c := &BarComputation{computations.NewDefaultComputation(), 0}
	computations.StartPlugin(c)
	for t := range c.Input() {
		t.Data = fmt.Sprintf("barized: %v", t.Data)
		c.Output() <- t
		c.counter++
	}
}
