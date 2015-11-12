package dagger

import (
	"fmt"
	"strings"
	"time"

	"github.com/nsaje/dagger/s"
	"github.com/rcrowley/go-metrics"
)

// Task is a unit of computation that consumes and/or produces a stream
// and can be replicated and synced across workers
type Task interface {
	TupleProcessor
	Run() error
	GetSnapshot() ([]byte, error)
	Sync() (time.Time, error)
	Stop()
}

// TaskManager manages tasks
type TaskManager struct {
	tasks       map[s.StreamID]Computation
	coordinator Coordinator
	receiver    *Receiver
	persister   Persister
	dispatcher  *Dispatcher

	counter metrics.Counter
}

// NewTaskManager creates a new task manager
func NewTaskManager(coordinator Coordinator,
	receiver *Receiver,
	persister Persister) *TaskManager {
	c := metrics.NewCounter()
	metrics.Register("processing", c)
	tm := &TaskManager{
		tasks:       make(map[s.StreamID]Computation),
		coordinator: coordinator,
		receiver:    receiver,
		persister:   persister,
		counter:     c,
	}
	receiver.SetTaskManager(tm)
	return tm
}

// ParseComputationID parses a computation definition
func ParseComputationID(s s.StreamID) (string, string, error) {
	c := strings.TrimSpace(string(s))
	firstParen := strings.Index(c, "(")
	if firstParen < 1 || c[len(c)-1] != ')' {
		return "", "", fmt.Errorf("Computation %s invalid!", c)
	}
	return c[:firstParen], c[firstParen+1 : len(c)-1], nil
}

// ManageTasks watches for new tasks and tries to acquire and run them
func (cm *TaskManager) ManageTasks() {

}

func (cm *TaskManager) setupTask(streamID s.StreamID) error {
	name, definition, err := ParseComputationID(streamID)
	if err != nil {
		return err
	}

	plugin, err := StartComputationPlugin(name, streamID)
	if err != nil {
		return err
	}

	// get information about the plugin, such as which input streams it needs
	info, err := plugin.GetInfo(definition)
	if err != nil {
		return err
	}

	var computation Computation
	if info.Stateful {
		computation, err = newStatefulComputation(streamID, cm.coordinator, cm.persister, plugin)
		if err != nil {
			return err
		}
	} else {
		computation = &statelessComputation{plugin, cm.dispatcher}
	}

	from, err := computation.Sync()
	if err != nil {
		return err
	}

	for _, input := range info.Inputs {
		cm.receiver.SubscribeTo(input, from, computation)
	}
	cm.tasks[streamID] = computation
	return nil
}

func (cm *TaskManager) GetSnapshot(streamID s.StreamID) (*s.ComputationSnapshot, error) {
	comp, has := cm.tasks[streamID]
	if !has {
		return nil, fmt.Errorf("Computation not found!")
	}
	return comp.GetSnapshot()
}
