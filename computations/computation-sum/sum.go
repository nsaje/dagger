package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/s"
	"github.com/twinj/uuid"
)

// SumProcessor calculates averages of numeric values over time periods
type SumProcessor struct {
	state SumCompState
}

// SumCompState contains state needed for this statistic
type SumCompState struct {
	Sums map[s.Timestamp]float64
}

// SumCompStateJSON modified for JSON transport
type SumCompStateJSON struct {
	Buckets []s.Timestamp
	Sums    []float64
}

// GetState returns serialized state for synchronization
func (c *SumProcessor) GetState() ([]byte, error) {
	stateJSON := NewSumProcessorStateJSON()
	for bucket := range c.state.Sums {
		stateJSON.Buckets = append(stateJSON.Buckets, bucket)
		stateJSON.Sums = append(stateJSON.Sums, c.state.Sums[bucket])
	}
	log.Println("returning stateJSON:", stateJSON)
	return json.Marshal(stateJSON)
}

// SetState deserializes and sets up a synchronized state for this computation
func (c *SumProcessor) SetState(state []byte) error {
	newState := NewSumProcessorState()
	newStateJSON := NewSumProcessorStateJSON()
	err := json.Unmarshal(state, &newStateJSON)
	if err != nil {
		return fmt.Errorf("Error setting plugin state: %s", err)
	}
	for i, bucket := range newStateJSON.Buckets {
		newState.Sums[bucket] = newStateJSON.Sums[i]
	}
	c.state = newState
	log.Println("[avg] new state:", c.state)
	return nil
}

// ProcessBucket updates the bucket with a new tuple
func (c *SumProcessor) ProcessBucket(bucket s.Timestamp, t *s.Tuple) error {
	log.Println("[avg] processing", t)
	value, _ := t.Data.(float64)
	c.state.Sums[bucket] += value
	return nil
}

// FinalizeBucket produces a new tuple from the bucket and deletes it
func (c *SumProcessor) FinalizeBucket(bucket s.Timestamp) *s.Tuple {
	log.Println("[avg] finalizing", bucket)
	new := &s.Tuple{
		Data:      c.state.Sums[bucket],
		Timestamp: bucket,
		ID:        uuid.NewV4().String(),
	}
	delete(c.state.Sums, bucket)
	log.Println("[avg] finalization finished", bucket)
	return new
}

// NewSumProcessorState initializes state struct
func NewSumProcessorState() SumCompState {
	return SumCompState{
		Sums: make(map[s.Timestamp]float64),
	}
}

// NewSumProcessorStateJSON initializes state struct for transfer via JSON
func NewSumProcessorStateJSON() SumCompStateJSON {
	return SumCompStateJSON{
		Buckets: make([]s.Timestamp, 0),
		Sums:    make([]float64, 0),
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
	log.SetPrefix("[SumComputation log] ")
	log.Printf("SumComputation started")
	c := computations.NewTimeBucketsComputation(&SumProcessor{state: NewSumProcessorState()})
	computations.StartPlugin(c)
}
