package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/dagger"
	"github.com/twinj/uuid"
)

// CountProcessor calculates averages of numeric values over time periods
type CountProcessor struct {
	state CountCompState
}

// CountCompState contains state needed for this statistic
type CountCompState struct {
	Sums   map[dagger.Timestamp]float64
	Counts map[dagger.Timestamp]int
}

// CountCompStateJSON modified for JSON transport
type CountCompStateJSON struct {
	Buckets []dagger.Timestamp
	Counts  []int
}

// GetState returns serialized state for synchronization
func (c *CountProcessor) GetState() ([]byte, error) {
	stateJSON := NewCountProcessorStateJSON()
	for bucket := range c.state.Counts {
		stateJSON.Buckets = append(stateJSON.Buckets, bucket)
		stateJSON.Counts = append(stateJSON.Counts, c.state.Counts[bucket])
	}
	log.Println("returning stateJSON:", stateJSON)
	return json.Marshal(stateJSON)
}

// SetState deserializes and sets up a synchronized state for this computation
func (c *CountProcessor) SetState(state []byte) error {
	newState := NewCountProcessorState()
	newStateJSON := NewCountProcessorStateJSON()
	err := json.Unmarshal(state, &newStateJSON)
	if err != nil {
		return fmt.Errorf("Error setting plugin state: %s", err)
	}
	for i, bucket := range newStateJSON.Buckets {
		newState.Counts[bucket] = newStateJSON.Counts[i]
	}
	c.state = newState
	log.Println("[avg] new state:", c.state)
	return nil
}

// ProcessBucket updates the bucket with a new record
func (c *CountProcessor) ProcessBucket(bucket dagger.Timestamp, t *dagger.Record) error {
	log.Println("[avg] processing", t)
	c.state.Counts[bucket]++
	return nil
}

// FinalizeBucket produces a new record from the bucket and deletes it
func (c *CountProcessor) FinalizeBucket(bucket dagger.Timestamp) *dagger.Record {
	log.Println("[avg] finalizing", bucket)
	new := &dagger.Record{
		Data:      c.state.Counts[bucket],
		Timestamp: bucket,
		ID:        uuid.NewV4().String(),
	}
	delete(c.state.Sums, bucket)
	log.Println("[avg] finalization finished", bucket)
	return new
}

// NewCountProcessorState initializes state struct
func NewCountProcessorState() CountCompState {
	return CountCompState{
		Counts: make(map[dagger.Timestamp]int),
	}
}

// NewCountProcessorStateJSON initializes state struct for transfer via JSON
func NewCountProcessorStateJSON() CountCompStateJSON {
	return CountCompStateJSON{
		Buckets: make([]dagger.Timestamp, 0),
		Counts:  make([]int, 0),
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
	log.SetPrefix("[CountComputation log] ")
	log.Printf("CountComputation started")
	c := computations.NewTimeBucketsComputation(&CountProcessor{state: NewCountProcessorState()})
	computations.StartPlugin(c)
}
