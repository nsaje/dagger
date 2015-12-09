package dagger

import (
	"errors"
	"log"
	"strings"
)

var errFormatIncorrect = errors.New("match format incorrect")

// NewMatchTask creates a new internal matching task that automatically
// subscribes to new matching strings
func NewMatchTask(c Coordinator, r InputManager, sid StreamID, definition string) (*TaskInfo, error) {
	topics, matchBy, streamID, err := parseMatchDefinition(definition)
	if err != nil {
		return nil, err
	}
	dispatcher := NewDispatcher(c)
	task := &matchTask{
		coordinator: c,
		receiver:    r,
		topics:      topics,
		matchBy:     matchBy,
		streamID:    streamID,
		dispatcher:  dispatcher,
		stopCh:      make(chan struct{}),
		existing:    make(map[string]struct{}),
	}
	taskInfo := &TaskInfo{
		StreamID: sid,
		Task:     task,
	}
	return taskInfo, nil
}

type matchTask struct {
	coordinator Coordinator
	receiver    InputManager
	topics      []StreamID
	matchBy     []string
	streamID    StreamID
	dispatcher  *Dispatcher

	existing map[string]struct{}
	stopCh   chan struct{}
}

func (mt *matchTask) GetSnapshot() ([]byte, error) { return nil, nil }
func (mt *matchTask) Sync() (Timestamp, error)     { return Timestamp(0), nil }

func (mt *matchTask) Run(chan error) {
	added := make(chan string)
	dropped := make(chan string)
	errc := make(chan error)
	for _, topic := range mt.topics {
		mt.coordinator.WatchTagMatch(topic, added, dropped, errc)
	}
	for {
		select {
		case <-mt.stopCh:
			return
		case pub := <-added:
			log.Println("ADDED", pub)
			pubTags := ParseTags(StreamID(pub))
			// pubTopic := StripTags(StreamID(pub))
			values := make([]string, len(mt.matchBy))
			for i, matchBy := range mt.matchBy {
				values[i] = pubTags[matchBy]
			}
			combination := strings.Join(values, "-")
			log.Println("combination", combination)
			_, existing := mt.existing[combination]
			if !existing {
				log.Println("not existing")
				mt.existing[combination] = struct{}{}
				for _, topic := range mt.topics {
					subTags := ParseTags(topic)
					subTopic := StripTags(topic)
					for i, matchBy := range mt.matchBy {
						subTags[matchBy] = values[i]
					}
					log.Println("subTags", subTags)
					sub := UnparseTags(subTopic, subTags)
					mt.receiver.SubscribeTo(sub, Timestamp(0), mt)
				}
			}
		}
	}
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
