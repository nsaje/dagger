package s

import (
	"fmt"
	"time"
)

// StreamID identifies a stream of tuples
type StreamID string

// Timestamp represents the timestamp of a dagger tuple
type Timestamp int64

// ToTime converts a timestamp into a Go's time object
func (t Timestamp) ToTime() time.Time {
	return time.Unix(0, int64(t))
}

// TSFromTime converts a Go's time object into a dagger timestamp
func TSFromTime(ts time.Time) Timestamp {
	return Timestamp(ts.UnixNano())
}

// Tuple is the atomic unit of data flowing through Dagger
type Tuple struct {
	ID        string      `json:"id"`
	StreamID  StreamID    `json:"stream_id"`
	LWM       Timestamp   `json:"lwm"`
	Timestamp Timestamp   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

func (t *Tuple) String() string {
	return fmt.Sprintln(t.StreamID, t.Timestamp, t.LWM, t.Data)
}

// ComputationPluginResponse  is returned from a computation plugin to the main app
type ComputationPluginResponse struct {
	Tuples []*Tuple
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

// ComputationSnapshot is used for synchronizing computation state between workers
type ComputationSnapshot struct {
	Received      []string
	InputBuffer   []*Tuple
	LastTimestamp Timestamp
	Produced      []*Tuple
	PluginState   *ComputationPluginState
}

func (s *ComputationSnapshot) String() string {
	return fmt.Sprintf("LastTimestamp: %v, InputBuffer: %v, Received: %v, Produced: %v, PluginState: %s",
		s.LastTimestamp,
		len(s.Received),
		len(s.InputBuffer),
		s.Produced,
		string(s.PluginState.State))
}
