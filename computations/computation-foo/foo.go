package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/s"
	"github.com/twinj/uuid"
)

// FooComputation simply prepends "fooized" to a record
type FooComputation struct {
	counter int
}

func (c *FooComputation) GetInfo(definition string) (s.ComputationPluginInfo, error) {
	info := s.ComputationPluginInfo{
		Inputs:   []s.StreamID{s.StreamID(definition)},
		Stateful: true,
	}
	return info, nil
}

func (c *FooComputation) GetState() ([]byte, error) {
	return []byte(fmt.Sprintf("%d", c.counter)), nil
}

func (c *FooComputation) SetState(state []byte) error {
	counter, err := strconv.Atoi(string(state))
	if err != nil {
		return fmt.Errorf("Error setting state %s in plugin: %s",
			string(state),
			err)
	}
	c.counter = counter
	return nil
}

func (c *FooComputation) SubmitTuple(t *s.Record) ([]*s.Record, error) {
	t.Data = fmt.Sprintf("fooized: %v, state: %v", t.Data, c.counter)
	t.ID = uuid.NewV4().String()
	c.counter++
	return []*s.Record{t}, nil
}

func main() {
	log.SetPrefix("[fooComputation log] ")
	log.Printf("fooComputation started")
	c := &FooComputation{1}
	computations.StartPlugin(c)
}
