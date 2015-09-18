package structs

import (
	"fmt"
	"time"
)

// Tuple is the atomic unit of data flowing through Dagger
type Tuple struct {
	ID       string      `json:"id"`
	StreamID string      `json:"stream_id"`
	LWM      time.Time   `json:"lwm"`
	Data     interface{} `json:"data"`
}

func (t *Tuple) String() string {
	return fmt.Sprint(*t)
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
	Produced    []*Tuple
	PluginState *ComputationPluginState
}

func (s *ComputationSnapshot) String() string {
	return fmt.Sprintf("Received: %v, Produced: %v, PluginState: %s",
		s.Received,
		s.Produced,
		string(s.PluginState.State))
}
