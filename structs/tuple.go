package structs

import (
	"fmt"
	"strings"
)

// Tuple is the atomic unit of data flowing through Dagger
type Tuple struct {
	ID       string      `json:"id"`
	StreamID string      `json:"stream_id"`
	Data     interface{} `json:"data"`
}

// ComputationResponse is returned from a computation plugin to the main app
type ComputationResponse struct {
	Tuples []Tuple
	State  interface{}
}

// ComputationPluginResponse  is returned from a computation plugin to the main app
type ComputationPluginResponse struct {
	Tuples []Tuple
}

// ComputationPluginInfo contains information about this computation plugin
type ComputationPluginInfo struct {
	Inputs   []string
	Stateful bool
}

// Computation represents info about an instance of a computation
type Computation struct {
	// The name identifying the computation type
	Name string
	// Free-form definition string, parsed by computations into arguments
	Definition string
}

// String returns textual representation of the computation
func (c Computation) String() string {
	return fmt.Sprintf("%s(%s)", c.Name, c.Definition)
}

// ComputationFromString parses a computation definition
func ComputationFromString(c string) (*Computation, error) {
	c = strings.TrimSpace(c)
	firstParen := strings.Index(c, "(")
	if firstParen < 1 || c[len(c)-1] != ')' {
		return nil, fmt.Errorf("Computation %s invalid!", c)
	}
	return &Computation{
		Name:       c[:firstParen],
		Definition: c[firstParen+1 : len(c)-1],
	}, nil
}
