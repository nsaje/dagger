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

type valueTable struct {
	values     map[string][]*structs.Tuple
	maxPeriods map[string]int
	LWM        time.Time
}

func newValueTable() valueTable {
	return valueTable{
		make(map[string][]*structs.Tuple),
		make(map[string]int),
		time.Time{},
	}
}

func (vt *valueTable) getLastN(streamID string, n int) []*structs.Tuple {
	timeSeries := vt.values[streamID]
	i := sort.Search(len(timeSeries), func(i int) bool {
		return timeSeries[i].Timestamp.After(vt.LWM)
	})
	if i < n {
		// not enough tuples in time series
		return nil
	}

	// delete old tuples
	deleteTo := i - vt.maxPeriods[streamID]
	if deleteTo > 0 {
		timeSeries = timeSeries[deleteTo:]
		vt.values[streamID] = timeSeries
	}

	return timeSeries[i-n : i]
}

func (vt *valueTable) insert(t *structs.Tuple) {
	timeSeries := vt.values[t.StreamID]
	vt.LWM = t.LWM

	// find the correct place for our tuple
	i := sort.Search(len(timeSeries), func(i int) bool {
		return timeSeries[i].Timestamp.After(t.Timestamp)
	})

	// insert it
	timeSeries = append(timeSeries, nil)
	copy(timeSeries[i+1:], timeSeries[i:])
	timeSeries[i] = t

	vt.values[t.StreamID] = timeSeries
}

type alarmComputationState struct {
	definition AlarmDefinition
	numInputs  int
	valueTable valueTable
}

func NewAlarmComputationState() alarmComputationState {
	return alarmComputationState{
		AlarmDefinition{},
		0,
		newValueTable(),
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

	c.state.valueTable.insert(t)
	fired, values := c.state.definition.tree.eval(c.state.valueTable)
	if fired {
		new := &structs.Tuple{
			Data: fmt.Sprintf("Alarm %+v fired with values %+v",
				c.state.definition.tree, values),
			Timestamp: t.Timestamp,
			ID:        uuid.NewV4().String(),
		}
		return []*structs.Tuple{new}, nil
	}

	// // evaluate if the bucket has all the necessary values for evaluation
	// if len(c.state.buckets[i].values) == c.state.numInputs {
	// 	fired := c.state.definition.tree.eval(c.state.buckets[i].values)
	// 	c.state.buckets[i].evaluated = true
	// 	c.state.buckets[i].fired = fired
	// 	if fired {
	// 		new := &structs.Tuple{
	// 			Data: fmt.Sprintf("Alarm %+v fired with values %v",
	// 				c.state.definition.tree, c.state.buckets[i].values),
	// 			Timestamp: t.Timestamp,
	// 			ID:        uuid.NewV4().String(),
	// 		}
	// 		return []*structs.Tuple{new}, nil
	// 	}
	// }

	return nil, nil
}

func main() {
	log.SetPrefix("[alarmComputation log] ")
	log.Printf("alarmComputation started")
	c := &AlarmComputation{NewAlarmComputationState()}
	computations.StartPlugin(c)
}
