package structs

import "fmt"

// Tuple is the atomic unit of data flowing through Dagger
type Tuple struct {
	ID       string      `json:"id"`
	StreamID string      `json:"stream_id"`
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

// ComputationSnapshot is used for synchronizing computation state between workers
type ComputationSnapshot struct {
	Received []string
	Produced []*Tuple
}
