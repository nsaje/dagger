package dagger

import (
	"errors"
	"strings"
)

var errFormatIncorrect = errors.New("match format incorrect")

// NewMatchTask creates a new internal matching task that automatically
// subscribes to new matching strings
func NewMatchTask(c Coordinator, sid StreamID, definition string) (*TaskInfo, error) {
	topics, matchBy, streamID, err := parseMatchDefinition(definition)
	if err != nil {
		return nil, err
	}
	dispatcher := NewDispatcher(c)
	task := &matchTask{c, topics, matchBy, streamID, dispatcher, make(chan struct{})}
	taskInfo := &TaskInfo{
		StreamID: sid,
		Task:     task,
	}
	return taskInfo, nil
}

type matchTask struct {
	c          Coordinator
	topics     []StreamID
	matchBy    []string
	streamID   StreamID
	dispatcher *Dispatcher
	stopCh     chan struct{}
}

func (mt *matchTask) GetSnapshot() ([]byte, error) { return nil, nil }
func (mt *matchTask) Sync() (Timestamp, error)     { return Timestamp(0), nil }

func (mt *matchTask) Run(chan error) {

}

func (mt *matchTask) Stop() {
	close(mt.stopCh)
}

func (mt *matchTask) ProcessRecord(rec *Record) error {
	return mt.dispatcher.ProcessRecord(rec)
}

func parseMatchDefinition(d string) ([]StreamID, []string, StreamID, error) {
	t0 := strings.Split(d, " by ")
	if len(t0) != 2 {
		return nil, nil, StreamID(""), errFormatIncorrect
	}
	t1 := strings.Split(t0[1], " in ")
	if len(t1) != 2 {
		return nil, nil, StreamID(""), errFormatIncorrect
	}
	topicsStr := strings.Split(t0[0], ",")
	if len(topicsStr) == 0 {
		return nil, nil, StreamID(""), errFormatIncorrect
	}
	topics := make([]StreamID, len(topicsStr))
	for i := range topicsStr {
		topics[i] = StreamID(strings.TrimSpace(topicsStr[i]))
	}

	matchBy := strings.Split(t1[0], ",")
	if len(matchBy) == 0 {
		return nil, nil, StreamID(""), errFormatIncorrect
	}
	for i := range matchBy {
		matchBy[i] = strings.TrimSpace(matchBy[i])
	}

	streamID := StreamID(strings.TrimSpace(t1[1]))
	return topics, matchBy, streamID, nil
}
