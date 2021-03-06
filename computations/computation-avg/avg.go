package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"

	log "github.com/Sirupsen/logrus"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/dagger"
	"github.com/twinj/uuid"
)

// AvgProcessor calculates averages of numeric values over time periods
type AvgProcessor struct {
	state AvgCompState
}

// AvgCompState contains state needed for this statistic
type AvgCompState struct {
	Sums   map[dagger.Timestamp]float64
	Counts map[dagger.Timestamp]int
}

// AvgCompStateJSON modified for JSON transport
type AvgCompStateJSON struct {
	Buckets []dagger.Timestamp
	Sums    []float64
	Counts  []int
}

// GetState returns serialized state for synchronization
func (c *AvgProcessor) GetState() ([]byte, error) {
	stateJSON := NewAvgProcessorStateJSON()
	for bucket := range c.state.Sums {
		stateJSON.Buckets = append(stateJSON.Buckets, bucket)
		stateJSON.Sums = append(stateJSON.Sums, c.state.Sums[bucket])
		stateJSON.Counts = append(stateJSON.Counts, c.state.Counts[bucket])
	}
	log.Println("returning stateJSON:", stateJSON)
	return json.Marshal(stateJSON)
}

// SetState deserializes and sets up a synchronized state for this computation
func (c *AvgProcessor) SetState(state []byte) error {
	newState := NewAvgProcessorState()
	newStateJSON := NewAvgProcessorStateJSON()
	err := json.Unmarshal(state, &newStateJSON)
	if err != nil {
		return fmt.Errorf("Error setting plugin state: %s", err)
	}
	for i, bucket := range newStateJSON.Buckets {
		newState.Sums[bucket] = newStateJSON.Sums[i]
		newState.Counts[bucket] = newStateJSON.Counts[i]
	}
	c.state = newState
	log.Println("[avg] new state:", c.state)
	return nil
}

// ProcessBucket updates the bucket with a new record
func (c *AvgProcessor) ProcessBucket(bucket dagger.Timestamp, t *dagger.Record) error {
	log.Println("[avg] processing", t)
	value, _ := strconv.ParseFloat(t.Data.(string), 64)
	c.state.Counts[bucket]++
	c.state.Sums[bucket] += value
	return nil
}

// FinalizeBucket produces a new record from the bucket and deletes it
func (c *AvgProcessor) FinalizeBucket(bucket dagger.Timestamp) *dagger.Record {
	log.Println("[avg] finalizing", bucket)
	new := &dagger.Record{
		Data:      c.state.Sums[bucket] / float64(c.state.Counts[bucket]),
		Timestamp: bucket,
		ID:        uuid.NewV4().String(),
	}
	delete(c.state.Sums, bucket)
	delete(c.state.Counts, bucket)
	log.Println("[avg] finalization finished", bucket)
	return new
}

// NewAvgProcessorState initializes state struct
func NewAvgProcessorState() AvgCompState {
	return AvgCompState{
		Sums:   make(map[dagger.Timestamp]float64),
		Counts: make(map[dagger.Timestamp]int),
	}
}

// NewAvgProcessorStateJSON initializes state struct for transfer via JSON
func NewAvgProcessorStateJSON() AvgCompStateJSON {
	return AvgCompStateJSON{
		Buckets: make([]dagger.Timestamp, 0),
		Sums:    make([]float64, 0),
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
	//log.SetPrefix("[AvgComputation log] ")
	log.Printf("AvgComputation started")
	c := computations.NewTimeBucketsComputation(&AvgProcessor{state: NewAvgProcessorState()})
	computations.StartPlugin(c)
}
