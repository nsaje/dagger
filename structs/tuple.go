package structs

import "sync"

// Tuple is the atomic unit of data flowing through Dagger
type Tuple struct {
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
