package dagger

import (
	"net"
	"time"
)

const (
	// JobPrefix is the key prefix for jobs
	JobPrefix = "dagger/jobs/"
)

// Tags are key-value metadata attached to a stream
type Tags map[string]string

// NewSubscriber encapsulates information about a new subscription
type NewSubscriber struct {
	Addr string
	From time.Time
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

// SubscribeCoordinator handles the act of subscribing to a stream
type SubscribeCoordinator interface {
	SubscribeTo(streamID StreamID, from time.Time) error
	CheckpointPosition(streamID StreamID, from time.Time) error
	UnsubscribeFrom(streamID StreamID) error
}

// PublishCoordinator handles the coordination of publishing a stream
type PublishCoordinator interface {
	GetSubscribers(streamID StreamID) ([]string, error) // DEPRECATE
	WatchSubscribers(streamID StreamID, stopCh chan struct{}) (chan NewSubscriber, chan string)
	WatchSubscriberPosition(topic StreamID, subscriber string, stopCh chan struct{}, position chan time.Time)
	RegisterAsPublisher(streamID StreamID)
}

// GroupHandler handles leadership status of a group
type GroupHandler interface {
	GetStatus() (bool, string, error)
}

// TaskCoordinator allows watching and taking new available tasks
type TaskCoordinator interface {
	// NewTaskWatcher creates a watcher that watches when new tasks show up or
	// are dropped
	NewTaskWatcher() TaskWatcher
	// AcquireTask tries to take a task from the task list. If another worker
	// manages to take the task, the call returns false.
	AcquireTask(StreamID) (bool, error)
	// TaskAcquired marks the task as successfuly acquired
	TaskAcquired(StreamID)
	// ReleaseTask releases the lock on the task so others can try to acquire it
	ReleaseTask(StreamID) (bool, error)
}

// TaskWatcher watches for and notifies of new available tasks
type TaskWatcher interface {
	Watcher
	New() chan StreamID
	Dropped() chan StreamID
}

// Watcher watches for changes in coordination store
type Watcher interface {
	Error() chan error
	Stop()
}

// ReplicationCoordinator coordinates replication of tuples onto multiple
// computations on multiple hosts for high availability
type ReplicationCoordinator interface {
	JoinGroup(streamID StreamID) (GroupHandler, error)
}
