package main

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/structs"
	"github.com/twinj/uuid"
)

type AlarmDefinition struct {
	tree    Node
	matchBy string
}

// AlarmComputation performs threshold alarming on numeric values
type AlarmComputation struct {
	state alarmComputationState
}

type Bucket struct {
	timestamp time.Time
	values    map[string]float64
	evaluated bool
	fired     bool
}

type alarmComputationState struct {
	definition AlarmDefinition
	numInputs  int
	buckets    []Bucket
}

func NewAlarmComputationState() alarmComputationState {
	return alarmComputationState{
		AlarmDefinition{},
		0,
		make([]Bucket, 0, 10),
	}
}

func (c *AlarmComputation) GetInfo(definition string) (structs.ComputationPluginInfo, error) {
	log.Println("parsing definition:", definition)
	parsed, err := Parse("alarmDefinition", []byte(definition))
	if err != nil {
		return structs.ComputationPluginInfo{},
			fmt.Errorf("Error parsing alarm definition: %s", err)
	}
	alarmDefinition := parsed.(AlarmDefinition)
	inputs := alarmDefinition.tree.getStreamIDs()
	c.state.definition = alarmDefinition
	c.state.numInputs = len(inputs)
	info := structs.ComputationPluginInfo{
		Inputs:   inputs,
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
	_, ok := t.Data.(float64)
	if !ok {
		return nil, fmt.Errorf("Wrong data format, expected float!")
	}

	// find the correct bucket in our sorted list of buckets
	i := sort.Search(len(c.state.buckets), func(i int) bool {
		return c.state.buckets[i].timestamp.After(t.Timestamp) ||
			c.state.buckets[i].timestamp.Equal(t.Timestamp)
	})
	// if it doesn't exist, create and insert it
	if i == len(c.state.buckets) || !c.state.buckets[i].timestamp.Equal(t.Timestamp) {
		b := Bucket{t.Timestamp, make(map[string]float64), false, false}
		c.state.buckets = append(c.state.buckets, Bucket{})
		copy(c.state.buckets[i+1:], c.state.buckets[i:])
		c.state.buckets[i] = b
	}

	// store the value inside bucket
	c.state.buckets[i].values[t.StreamID] = t.Data.(float64)

	// evaluate if the bucket has all the necessary values for evaluation
	if len(c.state.buckets[i].values) == c.state.numInputs {
		fired := c.state.definition.tree.eval(c.state.buckets[i].values)
		c.state.buckets[i].evaluated = true
		c.state.buckets[i].fired = fired
		if fired {
			new := &structs.Tuple{
				Data: fmt.Sprintf("Alarm %+v fired with values %v",
					c.state.definition.tree, c.state.buckets[i].values),
				Timestamp: t.Timestamp,
				ID:        uuid.NewV4().String(),
			}
			return []*structs.Tuple{new}, nil
		}
	}

	return nil, nil
}

func main() {
	log.SetPrefix("[alarmComputation log] ")
	log.Printf("alarmComputation started")
	c := &AlarmComputation{NewAlarmComputationState()}
	computations.StartPlugin(c)
}
