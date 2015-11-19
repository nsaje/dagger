package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/dagger"
	"github.com/twinj/uuid"
)

type alarmDefinition struct {
	tree    Node
	matchBy string
}

// AlarmComputation performs threshold alarming on numeric values
type AlarmComputation struct {
	state alarmComputationState
}

type valueTable struct {
	Values     map[dagger.StreamID][]*dagger.Record
	MaxPeriods map[dagger.StreamID]int
	LWM        dagger.Timestamp
}

func newValueTable() valueTable {
	return valueTable{
		make(map[dagger.StreamID][]*dagger.Record),
		make(map[dagger.StreamID]int),
		dagger.Timestamp(0),
	}
}

func (vt *valueTable) getLastN(streamID dagger.StreamID, n int) []*dagger.Record {
	timeSeries := vt.Values[streamID]
	i := sort.Search(len(timeSeries), func(i int) bool {
		return timeSeries[i].Timestamp > vt.LWM
	})
	if i < n {
		// not enough records in time series
		return nil
	}

	evalSlice := timeSeries[i-n : i]

	// delete old records
	deleteTo := i - vt.MaxPeriods[streamID]
	if deleteTo > 0 {
		timeSeries = timeSeries[deleteTo:]
		vt.Values[streamID] = timeSeries
	}

	return evalSlice
}

func (vt *valueTable) insert(t *dagger.Record) {
	timeSeries := vt.Values[t.StreamID]
	vt.LWM = t.LWM

	// find the correct place for our record
	i := sort.Search(len(timeSeries), func(i int) bool {
		return timeSeries[i].Timestamp > t.Timestamp
	})

	// insert it
	timeSeries = append(timeSeries, nil)
	copy(timeSeries[i+1:], timeSeries[i:])
	timeSeries[i] = t

	vt.Values[t.StreamID] = timeSeries
}

type alarmComputationState struct {
	definition       alarmDefinition
	StringDefinition string
	NumInputs        int
	ValueTable       valueTable
}

func NewAlarmComputationState() alarmComputationState {
	return alarmComputationState{
		alarmDefinition{},
		"",
		0,
		newValueTable(),
	}
}

func parseDefinition(definition string) (alarmDefinition, error) {
	parsed, err := Parse("alarmDefinition", []byte(definition))
	if err != nil {
		return alarmDefinition{},
			fmt.Errorf("Error parsing alarm definition '%s': %s", definition, err)
	}
	alarmDefinition := parsed.(alarmDefinition)
	return alarmDefinition, nil
}

func (c *AlarmComputation) GetInfo(definition string) (dagger.ComputationPluginInfo, error) {
	log.Println("parsing definition:", definition)
	alarmDefinition, err := parseDefinition(definition)
	if err != nil {
		return dagger.ComputationPluginInfo{}, err
	}
	leafNodes := alarmDefinition.tree.getLeafNodes()
	inputs := make([]dagger.StreamID, 0, len(leafNodes))
	maxPeriods := make(map[dagger.StreamID]int)
	for _, leafNode := range leafNodes {
		inputs = append(inputs, leafNode.streamID)
		currMax := maxPeriods[leafNode.streamID]
		if leafNode.periods > currMax {
			maxPeriods[leafNode.streamID] = leafNode.periods
		}
	}

	c.state.definition = alarmDefinition
	c.state.StringDefinition = definition
	c.state.NumInputs = len(inputs)
	c.state.ValueTable.MaxPeriods = maxPeriods
	info := dagger.ComputationPluginInfo{
		Inputs:   inputs,
		Stateful: true,
	}
	return info, nil
}

func (c *AlarmComputation) GetState() ([]byte, error) {
	return json.Marshal(c.state)
}

func (c *AlarmComputation) SetState(state []byte) error {
	newState := NewAlarmComputationState()
	err := json.Unmarshal(state, &newState)
	if err != nil {
		return err
	}
	alarmDefinition, err := parseDefinition(newState.StringDefinition)
	if err != nil {
		return err
	}
	newState.definition = alarmDefinition
	c.state = newState
	return nil
}

func (c *AlarmComputation) SubmitRecord(t *dagger.Record) ([]*dagger.Record, error) {
	_, ok := t.Data.(float64)
	if !ok {
		return nil, fmt.Errorf("Wrong data format, expected float!")
	}

	c.state.ValueTable.insert(t)
	fired, values := c.state.definition.tree.eval(c.state.ValueTable)
	if fired {
		new := &dagger.Record{
			Data: fmt.Sprintf("Alarm '%s' fired with values %+v",
				c.state.StringDefinition, values),
			Timestamp: t.Timestamp,
			ID:        uuid.NewV4().String(),
		}
		return []*dagger.Record{new}, nil
	} else {
		log.Printf("Alarm '%s' NOT fired with values %+v",
			c.state.StringDefinition, values)
	}

	// // evaluate if the bucket has all the necessary values for evaluation
	// if len(c.state.buckets[i].values) == c.state.numInputs {
	// 	fired := c.state.definition.tree.eval(c.state.buckets[i].values)
	// 	c.state.buckets[i].evaluated = true
	// 	c.state.buckets[i].fired = fired
	// 	if fired {
	// 		new := &dagger.Record{
	// 			Data: fmt.Sprintf("Alarm %+v fired with values %v",
	// 				c.state.definition.tree, c.state.buckets[i].values),
	// 			Timestamp: t.Timestamp,
	// 			ID:        uuid.NewV4().String(),
	// 		}
	// 		return []*dagger.Record{new}, nil
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
