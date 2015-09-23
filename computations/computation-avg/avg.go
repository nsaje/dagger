package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/structs"
	"github.com/twinj/uuid"
)

// AvgComputation calculates averages of numeric values over time periods
type AvgComputation struct {
	period time.Duration
	state  AvgCompState
}

type AvgCompState struct {
	sums       map[time.Time]float64
	counts     map[time.Time]int
	bucketList []time.Time
	lastLWM    time.Time
}

func (c *AvgComputation) GetInfo(definition string) (structs.ComputationPluginInfo, error) {
	var stream string
	var period string
	info := structs.ComputationPluginInfo{}
	info.Stateful = true

	// _, err := fmt.Sscanf(definition, "%s, %s", &stream, &period)
	// if err != nil {
	// 	return info, err
	// }
	tokens := strings.Split(definition, ",")
	if len(tokens) != 2 {
		return info, fmt.Errorf("Wrong format, expecting two parameters!")
	}
	stream = strings.TrimSpace(tokens[0])
	period = strings.TrimSpace(tokens[1])
	info.Inputs = []string{stream}

	p, err := time.ParseDuration(period)
	if err != nil {
		return info, fmt.Errorf("Wrong period format!", err)
	}
	c.period = p

	return info, nil
}

func (c *AvgComputation) GetState() ([]byte, error) {
	return json.Marshal(c.state)
}

func (c *AvgComputation) SetState(state []byte) error {
	newState := NewAvgComputationState()
	err := json.Unmarshal(state, &newState)
	if err != nil {
		return fmt.Errorf("Error setting plugin state", err)
	}
	c.state = newState
	return nil
}

func (c *AvgComputation) SubmitTuple(t *structs.Tuple) ([]*structs.Tuple, error) {
	bucket := t.Timestamp.Round(c.period)
	value, ok := t.Data.(float64)
	if !ok {
		return nil, fmt.Errorf("Wrong data format, expected float!")
	}
	if t.Timestamp.Before(c.state.lastLWM) {
		return nil, fmt.Errorf("LWM semantics violated! Tuple ts: %v, lastLWM: %v", t.Timestamp, c.state.lastLWM)
	}

	log.Println("putting in bucket", bucket)
	c.state.counts[bucket] += 1
	c.state.sums[bucket] += value

	var productions []*structs.Tuple

	for bucket, sum := range c.state.sums {
		if bucket.Add(c.period).Before(t.LWM) {
			new := &structs.Tuple{
				Data:      sum / float64(c.state.counts[bucket]),
				Timestamp: bucket,
				ID:        uuid.NewV4().String(),
			}
			productions = append(productions, new)
			delete(c.state.sums, bucket)
			delete(c.state.counts, bucket)
		}
	}
	c.state.lastLWM = t.LWM

	return productions, nil
}

func NewAvgComputationState() AvgCompState {
	return AvgCompState{
		sums:       make(map[time.Time]float64),
		counts:     make(map[time.Time]int),
		bucketList: make([]time.Time, 0),
	}
}

func main() {
	log.SetPrefix("[AvgComputation log] ")
	log.Printf("AvgComputation started")
	c := &AvgComputation{state: NewAvgComputationState()}
	computations.StartPlugin(c)
}
