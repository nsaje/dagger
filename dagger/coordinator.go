package dagger

import (
	"net"
	"time"

	"github.com/nsaje/dagger/dagger"
)

const (
	// JobPrefix is the key prefix for jobs
	JobPrefix = "dagger/jobs/"
)

// Tags are key-value metadata attached to a stream
type Tags map[string]string

// NewSubscriber encapsulates information about a new subscription
type NewSubscriber struct {
	addr string
	from time.Time
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
	SubscribeTo(streamID string, from time.Time) error
	CheckpointPosition(streamID string, from time.Time) error
	UnsubscribeFrom(streamID string) error
}

// PublishCoordinator handles the coordination of publishing a stream
type PublishCoordinator interface {
	GetSubscribers(streamID string) ([]string, error)
	WatchSubscribers(streamID string, stopCh chan struct{}) (chan dagger.NewSubscriber, chan string)
	WatchSubscriberPosition(topic string, subscriber string, stopCh chan struct{}, position chan time.Time)
	RegisterAsPublisher(streamID string)
}

// JobCoordinator coordinates accepts, starts and stops jobs DEPRECATED
type JobCoordinator interface {
	ManageJobs(ComputationManager)
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
	AcquireTask(Task) (bool, error)
	// TaskAcquired marks the task as successfuly acquired
	TaskAcquired(Task)
	// ReleaseTask releases the lock on the task so others can try to acquire it
	ReleaseTask(Task) (bool, error)
}

// TaskWatcher watches for and notifies of new available tasks
type TaskWatcher interface {
	Watcher
	New() chan Task
	Dropped() chan Task
}

// Watcher watches for changes in coordination store
type Watcher interface {
	Error() chan error
	Stop()
}

// ReplicationCoordinator coordinates replication of tuples onto multiple
// computations on multiple hosts for high availability
type ReplicationCoordinator interface {
	JoinGroup(streamID string) (GroupHandler, error)
}
