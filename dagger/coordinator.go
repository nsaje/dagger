package dagger

import (
	"net"
	"strings"

	"github.com/nsaje/dagger/s"
)

// Tags are key-value metadata attached to a stream
type Tags map[string]string

// NewSubscriber encapsulates information about a new subscription
type NewSubscriber struct {
	Addr string
	From s.Timestamp
}

// Coordinator coordinates topics, their publishers and subscribers
type Coordinator interface {
	// GetSubscribers(string) ([]string, error)
	// GetConfig(string) (string, error)
	SubscribeCoordinator
	PublishCoordinator
	TaskCoordinator
	ReplicationCoordinator
	Start(net.Addr) error
	Stop()
}

// TaskCoordinator allows watching and taking new available tasks
type TaskCoordinator interface {
	// WatchTasks creates a watcher that watches when new tasks show up or
	// are dropped
	WatchTasks(chan struct{}) (chan []string, chan error)
	// AcquireTask tries to take a task from the task list. If another worker
	// manages to take the task, the call returns false.
	AcquireTask(s.StreamID) (bool, error)
	// TaskAcquired marks the task as successfuly acquired
	TaskAcquired(s.StreamID)
	// ReleaseTask releases the lock on the task so others can try to acquire it
	ReleaseTask(s.StreamID) (bool, error)
}

// SubscribeCoordinator handles the act of subscribing to a stream
type SubscribeCoordinator interface {
	SubscribeTo(streamID s.StreamID, from s.Timestamp) error
	EnsurePublisherNum(s.StreamID, int, chan struct{}) chan error
	CheckpointPosition(streamID s.StreamID, from s.Timestamp) error
	UnsubscribeFrom(streamID s.StreamID) error
}

// PublishCoordinator handles the coordination of publishing a stream
type PublishCoordinator interface {
	GetSubscribers(streamID s.StreamID) ([]string, error) // DEPRECATE
	WatchSubscribers(s.StreamID, chan struct{}) (chan string, chan string, chan error)
	GetSubscriberPosition(s.StreamID, string) (s.Timestamp, error)
	WatchSubscriberPosition(s.StreamID, string, chan struct{}) (chan s.Timestamp, chan error)
	RegisterAsPublisher(streamID s.StreamID)
}

// GroupHandler handles leadership status of a group
type GroupHandler interface {
	GetStatus() (bool, string, error)
}

// ReplicationCoordinator coordinates replication of records onto multiple
// computations on multiple hosts for high availability
type ReplicationCoordinator interface {
	JoinGroup(streamID s.StreamID) (GroupHandler, error)
}

// ParseTags returns kv pairs encoded in stream ID
func ParseTags(s s.StreamID) Tags {
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

// StripTags removes the kv pairs encoded in s.StreamID
func StripTags(sid s.StreamID) s.StreamID {
	topic := string(sid)
	idx0 := strings.Index(topic, "{")
	if idx0 > 0 {
		return s.StreamID(topic[:idx0])
	}
	return s.StreamID(topic)
}
