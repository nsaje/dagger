package structs

import (
	"fmt"
	"strings"
	"sync"
)

// Tuple is the atomic unit of data flowing through Dagger
type Tuple struct {
	ID       string      `json:"id"`
	StreamID string      `json:"stream_id"`
	Data     interface{} `json:"data"`

	doneCh chan struct{}
	errCh  chan error

	sync.WaitGroup
}

// ProcessingStarted indicates that the tuple has entered the processing stage
func (t *Tuple) ProcessingStarted() (chan struct{}, chan error) {
	t.doneCh = make(chan struct{})
	t.errCh = make(chan error)
	return t.doneCh, t.errCh
}

// Ack marks tuple as successfuly processed, which means producers can be ACKed
func (t *Tuple) Ack() {
	close(t.doneCh)
}

// Fail means there was an error during tuple processing, notifies producers of the error
func (t *Tuple) Fail(err error) {
	t.errCh <- err
	close(t.errCh)
}

// ComputationResponse is returned from a computation plugin to the main app
type ComputationResponse struct {
	Tuples []Tuple
	State  interface{}
}

type InputsResponse struct {
	Inputs []string
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
