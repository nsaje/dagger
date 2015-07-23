package main

import (
	"fmt"
	"log"

	"bitbucket.org/nsaje/dagger/computations"
	"bitbucket.org/nsaje/dagger/structs"
)

// FooComputation simply prepends "fooized" to a tuple
type FooComputation struct{}

func (c FooComputation) GetInfo(definition string) (structs.ComputationPluginInfo, error) {
	info := structs.ComputationPluginInfo{
		Inputs:   []string{definition},
		Stateful: false,
	}
	return info, nil
}

func (c FooComputation) SubmitTuple(t *structs.Tuple) ([]*structs.Tuple, error) {
	t.Data = fmt.Sprintf("fooized: %v", t.Data)
	return []*structs.Tuple{t}, nil
}

func main() {
	log.SetPrefix("[fooComputation log] ")
	log.Printf("fooComputation started")
	c := FooComputation{}
	computations.StartPlugin(c)
}
