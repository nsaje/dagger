package dagger

import (
	"fmt"
	"log"
	"math/rand"
	"strings"

	"github.com/nsaje/dagger/s"
	"github.com/rcrowley/go-metrics"
)

// Task is a unit of computation that consumes and/or produces a stream
// and can be replicated and synced across workers
type Task interface {
	TupleProcessor
	Run() error
	GetSnapshot() ([]byte, error)
	Sync() (s.Timestamp, error)
	Stop()
}

// TaskManager manages tasks
type TaskManager struct {
	tasks       map[s.StreamID]Task
	coordinator Coordinator
	receiver    *Receiver
	persister   Persister
	dispatcher  *Dispatcher
	done        chan struct{}

	counter metrics.Counter
}

// NewTaskManager creates a new task manager
func NewTaskManager(coordinator Coordinator,
	receiver *Receiver,
	persister Persister) *TaskManager {
	c := metrics.NewCounter()
	metrics.Register("processing", c)
	tm := &TaskManager{
		tasks:       make(map[s.StreamID]Task),
		coordinator: coordinator,
		receiver:    receiver,
		persister:   persister,
		counter:     c,
		done:        make(chan struct{}),
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
	unapplicableSet := make(map[s.StreamID]struct{})
	w := cm.coordinator.NewTaskWatcher()
	for {
		select {
		case <-cm.done:
			return
		case err := <-w.Error():
			log.Println("[error] managetasks", err)
			return
		case candidateTasks := <-w.New():
			log.Println("got new tasks", candidateTasks)
			randomOrder := rand.Perm(len(candidateTasks))
			for _, i := range randomOrder {
				streamID := candidateTasks[i]
				if _, alreadyTaken := cm.tasks[streamID]; alreadyTaken {
					continue
				}
				if _, found := unapplicableSet[streamID]; found {
					continue
				}
				gotTask, err := cm.coordinator.AcquireTask(candidateTasks[i])
				if err != nil {
					// FIXME
					log.Println("[ERROR][coordinator]:", err)
					panic(err)
				}
				if gotTask {
					log.Println("[coordinator] Got task:", candidateTasks[i])
					err = cm.setupTask(streamID)
					if err != nil {
						log.Println("Error setting up computation:", err) // FIXME
						cm.coordinator.ReleaseTask(candidateTasks[i])     // FIXME ensure someone else tries to acquire
						unapplicableSet[streamID] = struct{}{}
						continue
					}
					// job set up successfuly, register as publisher and delete the job
					log.Println("[coordinator] Deleting job: ", streamID)
					cm.coordinator.TaskAcquired(candidateTasks[i])
					cm.coordinator.RegisterAsPublisher(streamID)
				}
			}
		}
	}
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

	var computation Task
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
	return computation.Run()
}

// GetSnapshot returns a snapshot of the requested task
func (cm *TaskManager) GetSnapshot(streamID s.StreamID) ([]byte, error) {
	comp, has := cm.tasks[streamID]
	if !has {
		return nil, fmt.Errorf("Computation not found!")
	}
	return comp.GetSnapshot()
}
