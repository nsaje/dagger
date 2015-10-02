package main

import (
	"fmt"
	"log"

	"github.com/nsaje/dagger/structs"
)

type AlarmDefinition struct {
	tree    Node
	matchBy string
}

// AlarmComputation performs threshold alarming on numeric values
type AlarmComputation struct {
	counter int
	state   alarmComputationState
}

type alarmComputationState struct {
}

func (c *AlarmComputation) GetInfo(definition string) (structs.ComputationPluginInfo, error) {
	tree, err := Parse("alarmDefinition", []byte(definition))
	if err != nil {
		return structs.ComputationPluginInfo{},
			fmt.Errorf("Error parsing alarm definition: %s", err)
	}
	info := structs.ComputationPluginInfo{
		Inputs:   tree.(Node).getStreamIDs(),
		Stateful: true,
	}
	return info, nil
}

func (c *AlarmComputation) GetState() ([]byte, error) {
	return []byte{}, nil
}

func (c *AlarmComputation) SetState(state []byte) error {
	return nil
}

func (c *AlarmComputation) SubmitTuple(t *structs.Tuple) ([]*structs.Tuple, error) {

	return nil, nil
}

func main() {
	log.SetPrefix("[fooComputation log] ")
	log.Printf("fooComputation started")
	// c := &AlarmComputation{1}
	// computations.StartPlugin(c)
}
