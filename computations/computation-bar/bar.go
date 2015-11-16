package main

import (
	"fmt"
	"log"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/s"
)

// BarComputation simply prepends "barized" to a record
type BarComputation struct{}

func (c BarComputation) GetInfo(definition string) (s.ComputationPluginInfo, error) {
	info := s.ComputationPluginInfo{
		Inputs:   []s.StreamID{s.StreamID(definition)},
		Stateful: false,
	}
	return info, nil
}

func (c BarComputation) SubmitTuple(t *s.Record) ([]*s.Record, error) {
	t.Data = fmt.Sprintf("barized: %v", t.Data)
	return []*s.Record{t}, nil
}

func (c BarComputation) GetState() ([]byte, error) {
	return nil, fmt.Errorf("not stateful!")
}

func (c BarComputation) SetState(state []byte) error {
	return fmt.Errorf("not stateful!")
}

func main() {
	log.SetPrefix("[barComputation log] ")
	log.Printf("barComputation started")
	c := BarComputation{}
	computations.StartPlugin(c)
}
