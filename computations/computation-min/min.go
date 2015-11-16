package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/s"
	"github.com/twinj/uuid"
)

// MinProcessor calculates minimum of numeric values over time periods
type MinProcessor struct {
	state MinCompState
}

// MinCompState contains state needed for this statistic
type MinCompState struct {
	Values map[s.Timestamp][]float64
}

// MinCompStateJSON modified for JSON transport
type MinCompStateJSON struct {
	Buckets []s.Timestamp
	Values  [][]float64
}

// GetState returns serialized state for synchronization
func (c *MinProcessor) GetState() ([]byte, error) {
	stateJSON := NewMinProcessorStateJSON()
	for bucket := range c.state.Values {
		stateJSON.Buckets = append(stateJSON.Buckets, bucket)
		stateJSON.Values = append(stateJSON.Values, c.state.Values[bucket])
	}
	log.Println("returning stateJSON:", stateJSON)
	return json.Marshal(stateJSON)
}

// SetState deserializes and sets up a synchronized state for this computation
func (c *MinProcessor) SetState(state []byte) error {
	newState := NewMinProcessorState()
	newStateJSON := NewMinProcessorStateJSON()
	err := json.Unmarshal(state, &newStateJSON)
	if err != nil {
		return fmt.Errorf("Error setting plugin state: %s", err)
	}
	for i, bucket := range newStateJSON.Buckets {
		newState.Values[bucket] = newStateJSON.Values[i]
	}
	c.state = newState
	log.Println("[Min] new state:", c.state)
	return nil
}

// ProcessBucket updates the bucket with a new record
func (c *MinProcessor) ProcessBucket(bucket s.Timestamp, t *s.Record) error {
	log.Println("[Min] processing", t)
	value, _ := t.Data.(float64)
	c.state.Values[bucket] = append(c.state.Values[bucket], value)
	return nil
}

// FinalizeBucket produces a new record from the bucket and deletes it
func (c *MinProcessor) FinalizeBucket(bucket s.Timestamp) *s.Record {
	log.Println("[Min] finalizing", bucket)
	min := math.Inf(1)
	for _, v := range c.state.Values[bucket] {
		if v < min {
			min = v
		}
	}
	new := &s.Record{
		Data:      min,
		Timestamp: bucket,
		ID:        uuid.NewV4().String(),
	}
	delete(c.state.Values, bucket)
	log.Println("[Min] finalization finished", bucket)
	return new
}

// NewMinProcessorState initializes state struct
func NewMinProcessorState() MinCompState {
	return MinCompState{
		Values: make(map[s.Timestamp][]float64),
	}
}

// NewMinProcessorStateJSON initializes state struct for transfer via JSON
func NewMinProcessorStateJSON() MinCompStateJSON {
	return MinCompStateJSON{
		Buckets: make([]s.Timestamp, 0),
		Values:  make([][]float64, 0),
	}
}

func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		for sig := range ch {
			panic(sig)
		}
	}()
	log.SetPrefix("[MinComputation log] ")
	log.Printf("MinComputation started")
	c := computations.NewTimeBucketsComputation(&MinProcessor{state: NewMinProcessorState()})
	computations.StartPlugin(c)
}
