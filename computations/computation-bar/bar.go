package main

import (
	"fmt"
	"log"

	"bitbucket.org/nsaje/dagger/computations"
	"bitbucket.org/nsaje/dagger/structs"
)

// BarComputation simply prepends "barized" to a tuple
type BarComputation struct{}

func (c BarComputation) GetInfo(definition string) (structs.ComputationPluginInfo, error) {
	info := structs.ComputationPluginInfo{
		Inputs:   []string{definition},
		Stateful: false,
	}
	return info, nil
}

func (c BarComputation) SubmitTuple(t *structs.Tuple) ([]*structs.Tuple, error) {
	t.Data = fmt.Sprintf("barized: %v", t.Data)
	return []*structs.Tuple{t}, nil
}

func main() {
	log.SetPrefix("[barComputation log] ")
	log.Printf("barComputation started")
	c := BarComputation{}
	computations.StartPlugin(c)
}
