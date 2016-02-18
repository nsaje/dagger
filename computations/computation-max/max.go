package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"math"
	"os"
	"os/signal"
	"strconv"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/dagger"
	"github.com/twinj/uuid"
)

// MaxProcessor calculates minimum of numeric values over time periods
type MaxProcessor struct {
	state MaxCompState
}

// MaxCompState contains state needed for this statistic
type MaxCompState struct {
	Values map[dagger.Timestamp][]float64
}

// MaxCompStateJSON modified for JSON transport
type MaxCompStateJSON struct {
	Buckets []dagger.Timestamp
	Values  [][]float64
}

// GetState returns serialized state for synchronization
func (c *MaxProcessor) GetState() ([]byte, error) {
	stateJSON := NewMaxProcessorStateJSON()
	for bucket := range c.state.Values {
		stateJSON.Buckets = append(stateJSON.Buckets, bucket)
		stateJSON.Values = append(stateJSON.Values, c.state.Values[bucket])
	}
	log.Println("returning stateJSON:", stateJSON)
	return json.Marshal(stateJSON)
}

// SetState deserializes and sets up a synchronized state for this computation
func (c *MaxProcessor) SetState(state []byte) error {
	newState := NewMaxProcessorState()
	newStateJSON := NewMaxProcessorStateJSON()
	err := json.Unmarshal(state, &newStateJSON)
	if err != nil {
		return fmt.Errorf("Error setting plugin state: %s", err)
	}
	for i, bucket := range newStateJSON.Buckets {
		newState.Values[bucket] = newStateJSON.Values[i]
	}
	c.state = newState
	log.Println("[Max] new state:", c.state)
	return nil
}

// ProcessBucket updates the bucket with a new record
func (c *MaxProcessor) ProcessBucket(bucket dagger.Timestamp, t *dagger.Record) error {
	log.Println("[Max] processing", t)
	value, _ := strconv.ParseFloat(t.Data.(string), 64)
	c.state.Values[bucket] = append(c.state.Values[bucket], value)
	return nil
}

// FinalizeBucket produces a new record from the bucket and deletes it
func (c *MaxProcessor) FinalizeBucket(bucket dagger.Timestamp) *dagger.Record {
	log.Println("[Max] finalizing", bucket)
	max := math.Inf(-1)
	for _, v := range c.state.Values[bucket] {
		if v > max {
			max = v
		}
	}
	new := &dagger.Record{
		Data:      max,
		Timestamp: bucket,
		ID:        uuid.NewV4().String(),
	}
	delete(c.state.Values, bucket)
	log.Println("[Max] finalization finished", bucket)
	return new
}

// NewMaxProcessorState initializes state struct
func NewMaxProcessorState() MaxCompState {
	return MaxCompState{
		Values: make(map[dagger.Timestamp][]float64),
	}
}

// NewMaxProcessorStateJSON initializes state struct for transfer via JSON
func NewMaxProcessorStateJSON() MaxCompStateJSON {
	return MaxCompStateJSON{
		Buckets: make([]dagger.Timestamp, 0),
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
	//log.SetPrefix("[MaxComputation log] ")
	log.Printf("MaxComputation started")
	c := computations.NewTimeBucketsComputation(&MaxProcessor{state: NewMaxProcessorState()})
	computations.StartPlugin(c)
}
