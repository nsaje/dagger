package main

import (
	"fmt"
	"log"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/structs"
	"github.com/twinj/uuid"
)

// FooComputation simply prepends "fooized" to a tuple
type FooComputation struct{}

func (c FooComputation) GetInfo(definition string) (structs.ComputationPluginInfo, error) {
	info := structs.ComputationPluginInfo{
		Inputs:   []string{definition},
		Stateful: true,
	}
	return info, nil
}

func (c FooComputation) SubmitTuple(t *structs.Tuple) ([]*structs.Tuple, error) {
	t.Data = fmt.Sprintf("fooized: %v", t.Data)
	t.ID = uuid.NewV4().String()
	return []*structs.Tuple{t}, nil
}

func main() {
	log.SetPrefix("[fooComputation log] ")
	log.Printf("fooComputation started")
	c := FooComputation{}
	computations.StartPlugin(c)
}
