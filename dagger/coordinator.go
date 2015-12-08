package dagger

import (
	"net"
	"strings"
)

// Tags are key-value metadata attached to a stream
type Tags map[string]string

// NewSubscriber encapsulates information about a new subscription
type NewSubscriber struct {
	Addr string
	From Timestamp
}

// Coordinator coordinates topics, their publishers and subscribers
type Coordinator interface {
	// GetSubscribers(string) ([]string, error)
	// GetConfig(string) (string, error)
	SubscribeCoordinator
	PublishCoordinator
	TaskCoordinator
	ReplicationCoordinator
	Start(net.Addr, chan error) error
	Stop()
}

// TaskCoordinator allows watching and taking new available tasks
type TaskCoordinator interface {
	// WatchTasks creates a watcher that watches when new tasks show up or
	// are dropped
	WatchTasks(chan struct{}) (chan []string, chan error)
	// AcquireTask tries to take a task from the task list. If another worker
	// manages to take the task, the call returns false.
	AcquireTask(StreamID) (bool, error)
	// TaskAcquired marks the task as successfuly acquired
	TaskAcquired(StreamID) error
	// ReleaseTask releases the lock on the task so others can try to acquire it
	ReleaseTask(StreamID) (bool, error)
}

// SubscribeCoordinator handles the act of subscribing to a stream
type SubscribeCoordinator interface {
	SubscribeTo(streamID StreamID, from Timestamp) error
	EnsurePublisherNum(StreamID, int, chan struct{}) chan error
	CheckpointPosition(streamID StreamID, from Timestamp) error
	UnsubscribeFrom(streamID StreamID) error
	WatchTagMatch(topic string, tags Tags, matcherTags []string, addedRet chan string, droppedRet chan string, errc chan error)
}

// PublishCoordinator handles the coordination of publishing a stream
type PublishCoordinator interface {
	GetSubscribers(streamID StreamID) ([]string, error) // DEPRECATE
	WatchSubscribers(StreamID, chan struct{}) (chan string, chan string, chan error)
	GetSubscriberPosition(StreamID, string) (Timestamp, error)
	WatchSubscriberPosition(StreamID, string, chan struct{}) (chan Timestamp, chan error)
	RegisterAsPublisher(streamID StreamID) error
	DeregisterAsPublisher(streamID StreamID) error
}

// GroupHandler handles leadership status of a group
type GroupHandler interface {
	GetStatus() (bool, string, error)
}

// ReplicationCoordinator coordinates replication of records onto multiple
// computations on multiple hosts for high availability
type ReplicationCoordinator interface {
	JoinGroup(streamID StreamID) (GroupHandler, error)
}

// ParseTags returns kv pairs encoded in stream ID
func ParseTags(s StreamID) Tags {
	topic := string(s)
	tags := make(Tags)
	idx0 := strings.Index(topic, "{")
	idx1 := strings.Index(topic, "}")
	if idx0 == -1 || idx1 == -1 {
		return nil
	}
	taglist := topic[idx0+1 : idx1]
	pairs := strings.Split(taglist, ",")
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			continue
		}
		tags[kv[0]] = kv[1]
	}
	if len(tags) == 0 {
		return nil
	}
	return tags
}

// StripTags removes the kv pairs encoded in StreamID
func StripTags(sid StreamID) StreamID {
	topic := string(sid)
	idx0 := strings.Index(topic, "{")
	if idx0 > 0 {
		return StreamID(topic[:idx0])
	}
	return StreamID(topic)
}
