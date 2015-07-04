package structs

// Tuple is the atomic unit of data flowing through Dagger
type Tuple struct {
	StreamID string      `json:"stream_id"`
	Data     interface{} `json:"data"`
}

// ComputationResponse is returned from a computation plugin to the main app
type ComputationResponse struct {
	Tuples []Tuple
	State  interface{}
}
