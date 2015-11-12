package computations

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nsaje/dagger/s"
)

type TimeBucketsProcessor interface {
	ProcessBucket(bucket time.Time, t *s.Tuple) error
	FinalizeBucket(bucket time.Time) *s.Tuple
	GetState() ([]byte, error)
	SetState([]byte) error
}

type TimeBucketsState struct {
	LastLWM        time.Time        `json:"last_lwm"`
	ProcessorState *json.RawMessage `json:"processor_state"`
}

type TimeBucketsComputation struct {
	period    time.Duration
	buckets   map[time.Time]struct{}
	processor TimeBucketsProcessor
	lastLWM   time.Time
}

func NewTimeBucketsComputation(processor TimeBucketsProcessor) *TimeBucketsComputation {
	return &TimeBucketsComputation{
		processor: processor,
		buckets:   make(map[time.Time]struct{}),
	}
}

func (c *TimeBucketsComputation) GetInfo(definition string) (s.ComputationPluginInfo, error) {
	var stream string
	var period string
	info := s.ComputationPluginInfo{}
	info.Stateful = true

	tokens := strings.Split(definition, ",")
	if len(tokens) != 2 {
		return info, fmt.Errorf("Wrong format, expecting two parameters!")
	}
	stream = strings.TrimSpace(tokens[0])
	period = strings.TrimSpace(tokens[1])
	info.Inputs = []s.StreamID{s.StreamID(stream)}

	p, err := time.ParseDuration(period)
	if err != nil {
		return info, fmt.Errorf("Wrong period format!", err)
	}
	c.period = p

	return info, nil
}

func (c *TimeBucketsComputation) GetState() ([]byte, error) {
	log.Println("[computation] getting processor state")
	var processorState json.RawMessage
	processorState, err := c.processor.GetState()
	if err != nil {
		return []byte{}, fmt.Errorf("Error getting state: %s", err)
	}
	json, err := json.Marshal(TimeBucketsState{c.lastLWM, &processorState})
	if err != nil {
		return []byte{}, fmt.Errorf("JSON marshalling error while getting state: %s", err)
	}
	log.Println("[time_buckets] returning state", string(json), c.lastLWM, processorState)
	return json, nil
}

func (c *TimeBucketsComputation) SetState(state []byte) error {
	var newState TimeBucketsState
	err := json.Unmarshal(state, &newState)
	if err != nil {
		return fmt.Errorf("Error setting plugin state", err)
	}
	c.lastLWM = newState.LastLWM
	log.Println("[time_buckets] new LWM:", c.lastLWM)
	err = c.processor.SetState(*newState.ProcessorState)
	if err != nil {
		return fmt.Errorf("Error setting state: %s", err)
	}
	return nil
}

func (c *TimeBucketsComputation) SubmitTuple(t *s.Tuple) ([]*s.Tuple, error) {
	log.Println("[time_buckets] processing tuple", t)
	bucket := t.Timestamp.Round(c.period)
	_, ok := t.Data.(float64)
	if !ok {
		return nil, fmt.Errorf("Wrong data format, expected float!")
	}
	if t.Timestamp.Before(c.lastLWM) {
		return nil, fmt.Errorf("LWM semantics violated! Tuple ts: %v, lastLWM: %v", t.Timestamp, c.lastLWM)
	}

	c.buckets[bucket] = struct{}{}
	log.Println("[time_buckets] submitting to processor ", t)
	c.processor.ProcessBucket(bucket, t)

	var productions []*s.Tuple

	for bucket, _ := range c.buckets {
		if bucket.Add(c.period).Before(t.LWM) {
			log.Println("bucket %s before LWM %s!", bucket, t.LWM)
			new := c.processor.FinalizeBucket(bucket)
			delete(c.buckets, bucket)
			productions = append(productions, new)
		} else {
			log.Println("bucket %s NOT before LWM %s!", bucket, t.LWM)
		}
	}
	c.lastLWM = t.LWM

	log.Println("[time_buckets] returning from RPC func", productions)
	return productions, nil
}
