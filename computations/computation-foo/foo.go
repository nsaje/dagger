package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/dagger"
	"github.com/twinj/uuid"
)

// FooComputation simply prepends "fooized" to a record
type FooComputation struct {
	counter int
}

func (c *FooComputation) GetInfo(definition string) (dagger.ComputationPluginInfo, error) {
	info := dagger.ComputationPluginInfo{
		Inputs:   []dagger.StreamID{dagger.StreamID(definition)},
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

func (c *FooComputation) SubmitRecord(t *dagger.Record) ([]*dagger.Record, error) {
	t.Data = fmt.Sprintf("fooized: %v, state: %v", t.Data, c.counter)
	t.ID = uuid.NewV4().String()
	c.counter++
	return []*dagger.Record{t}, nil
}

func main() {
	log.SetPrefix("[fooComputation log] ")
	log.Printf("fooComputation started")
	c := &FooComputation{1}
	computations.StartPlugin(c)
}
