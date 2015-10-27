package structs

import (
	"fmt"
	"time"
)

// Tuple is the atomic unit of data flowing through Dagger
type Tuple struct {
	ID        string      `json:"id"`
	StreamID  string      `json:"stream_id"`
	LWM       time.Time   `json:"lwm"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

func (t *Tuple) String() string {
	// return fmt.Sprintf("stream_id: %s, data: %v", t.StreamID, t.Data)
	return fmt.Sprintf("lwm: %s, timestamp:%s, data: %v", t.LWM, t.Timestamp, t.Data)
}

// ComputationPluginResponse  is returned from a computation plugin to the main app
type ComputationPluginResponse struct {
	Tuples []*Tuple
}

// ComputationPluginInfo contains information about this computation plugin
type ComputationPluginInfo struct {
	Inputs   []string
	Stateful bool
}

// ComputationPluginState is state returned from a computation plugin to the main app
type ComputationPluginState struct {
	State []byte
}

// ComputationSnapshot is used for synchronizing computation state between workers
type ComputationSnapshot struct {
	Received    []string
	InputBuffer []*Tuple
	Produced    []*Tuple
	PluginState *ComputationPluginState
}

func (s *ComputationSnapshot) String() string {
	return fmt.Sprintf("InputBuffer: %v, Received: %v, Produced: %v, PluginState: %s",
		len(s.Received),
		len(s.InputBuffer),
		s.Produced,
		string(s.PluginState.State))
}
