package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/s"
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
	Values     map[s.StreamID][]*s.Record
	MaxPeriods map[s.StreamID]int
	LWM        s.Timestamp
}

func newValueTable() valueTable {
	return valueTable{
		make(map[s.StreamID][]*s.Record),
		make(map[s.StreamID]int),
		s.Timestamp(0),
	}
}

func (vt *valueTable) getLastN(streamID s.StreamID, n int) []*s.Record {
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

func (vt *valueTable) insert(t *s.Record) {
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

func (c *AlarmComputation) GetInfo(definition string) (s.ComputationPluginInfo, error) {
	log.Println("parsing definition:", definition)
	alarmDefinition, err := parseDefinition(definition)
	if err != nil {
		return s.ComputationPluginInfo{}, err
	}
	leafNodes := alarmDefinition.tree.getLeafNodes()
	inputs := make([]s.StreamID, 0, len(leafNodes))
	maxPeriods := make(map[s.StreamID]int)
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
	info := s.ComputationPluginInfo{
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

func (c *AlarmComputation) SubmitTuple(t *s.Record) ([]*s.Record, error) {
	_, ok := t.Data.(float64)
	if !ok {
		return nil, fmt.Errorf("Wrong data format, expected float!")
	}

	c.state.ValueTable.insert(t)
	fired, values := c.state.definition.tree.eval(c.state.ValueTable)
	if fired {
		new := &s.Record{
			Data: fmt.Sprintf("Alarm '%s' fired with values %+v",
				c.state.StringDefinition, values),
			Timestamp: t.Timestamp,
			ID:        uuid.NewV4().String(),
		}
		return []*s.Record{new}, nil
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
	// 		new := &s.Tuple{
	// 			Data: fmt.Sprintf("Alarm %+v fired with values %v",
	// 				c.state.definition.tree, c.state.buckets[i].values),
	// 			Timestamp: t.Timestamp,
	// 			ID:        uuid.NewV4().String(),
	// 		}
	// 		return []*s.Tuple{new}, nil
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
