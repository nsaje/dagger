package dagger

import (
	"fmt"
	"strconv"
	"time"
)

var _timestampDiff = time.Now().UnixNano()

// StreamID identifies a stream of records
type StreamID string

// Timestamp represents the timestamp of a dagger record
type Timestamp int64

func (t Timestamp) String() string {
	// return fmt.Sprint(int64(t) - _timestampDiff)
	return fmt.Sprintf("%d", int64(t))
}

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
	ID string `json:"id"`

	StreamID  StreamID    `json:"stream_id"`
	LWM       Timestamp   `json:"lwm"`
	Timestamp Timestamp   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

func (t *Record) String() string {
	return fmt.Sprintf("%s %s %s %d: %s", t.StreamID, t.Timestamp, t.LWM, int64(t.Timestamp-t.LWM), t.Data)
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
