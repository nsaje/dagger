package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/dagger"
)

// BarComputation simply prepends "barized" to a record
type BarComputation struct{}

func (c BarComputation) GetInfo(definition string) (dagger.ComputationPluginInfo, error) {
	info := dagger.ComputationPluginInfo{
		Inputs:   []dagger.StreamID{dagger.StreamID(definition)},
		Stateful: false,
	}
	return info, nil
}

func (c BarComputation) SubmitRecord(t *dagger.Record) ([]*dagger.Record, error) {
	t.Data = fmt.Sprintf("barized: %v", t.Data)
	return []*dagger.Record{t}, nil
}

func (c BarComputation) GetState() ([]byte, error) {
	return nil, fmt.Errorf("not stateful!")
}

func (c BarComputation) SetState(state []byte) error {
	return fmt.Errorf("not stateful!")
}

func main() {
	//log.SetPrefix("[barComputation log] ")
	log.Printf("barComputation started")
	c := BarComputation{}
	computations.StartPlugin(c)
}
