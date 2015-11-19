package dagger

import (
	"fmt"
	"strconv"
	"time"
)

// StreamID identifies a stream of records
type StreamID string

// Timestamp represents the timestamp of a dagger record
type Timestamp int64

// ToTime converts a timestamp into a Go's time object
func (t Timestamp) ToTime() time.Time {
	return time.Unix(0, int64(t))
}

// TSFromTime converts a Go's time object into a dagger timestamp
func TSFromTime(ts time.Time) Timestamp {
	return Timestamp(ts.UnixNano())
}

// TSFromString parses a stringified int64 into a dagger timestamp
func TSFromString(ts string) Timestamp {
	posNsec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		posNsec = 0
	}
	return Timestamp(posNsec)
}

// Record is the atomic unit of data flowing through Dagger
type Record struct {
	ID        string      `json:"id"`
	StreamID  StreamID    `json:"stream_id"`
	LWM       Timestamp   `json:"lwm"`
	Timestamp Timestamp   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

func (t *Record) String() string {
	return fmt.Sprintln(t.StreamID, t.Timestamp, t.LWM, t.Data)
}

// ComputationPluginResponse  is returned from a computation plugin to the main app
type ComputationPluginResponse struct {
	Records []*Record
}

// ComputationPluginInfo contains information about this computation plugin
type ComputationPluginInfo struct {
	Inputs   []StreamID
	Stateful bool
}

// ComputationPluginState is state returned from a computation plugin to the main app
type ComputationPluginState struct {
	State []byte
}

// TaskSnapshot is used for synchronizing task state between workers
type TaskSnapshot struct {
	Received      []string
	InputBuffer   []*Record
	LastTimestamp Timestamp `json:",string"`
	Produced      []*Record
	PluginState   *ComputationPluginState
}

func (s *TaskSnapshot) String() string {
	return fmt.Sprintf("LastTimestamp: %v, InputBuffer: %v, Received: %v, Produced: %v, PluginState: %s",
		s.LastTimestamp,
		len(s.Received),
		len(s.InputBuffer),
		s.Produced,
		string(s.PluginState.State))
}
