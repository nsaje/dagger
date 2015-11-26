package dagger

import (
	"fmt"
	"log"
	"math/rand"
	"strings"

	"github.com/rcrowley/go-metrics"
)

// Task is a unit of computation that consumes and/or produces a stream
// and can be replicated and synced across workers
type Task interface {
	RecordProcessor
	Run(chan error)
	GetSnapshot() ([]byte, error)
	Sync() (Timestamp, error)
	Stop()
}

// TaskManager manages tasks
type TaskManager struct {
	tasks        map[StreamID]Task
	coordinator  Coordinator
	receiver     *Receiver
	persister    Persister
	dispatcher   *Dispatcher
	done         chan struct{}
	taskFailedCh chan Task

	counter metrics.Counter
}

// NewTaskManager creates a new task manager
func NewTaskManager(coordinator Coordinator,
	receiver *Receiver,
	persister Persister) *TaskManager {
	c := metrics.NewCounter()
	metrics.Register("processing", c)
	tm := &TaskManager{
		tasks:       make(map[StreamID]Task),
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
func ParseComputationID(s StreamID) (string, string, error) {
	c := strings.TrimSpace(string(s))
	firstParen := strings.Index(c, "(")
	if firstParen < 1 || c[len(c)-1] != ')' {
		return "", "", fmt.Errorf("Computation %s invalid!", c)
	}
	return c[:firstParen], c[firstParen+1 : len(c)-1], nil
}

// ManageTasks watches for new tasks and tries to acquire and run them
func (cm *TaskManager) ManageTasks() error {
	unapplicableSet := make(map[StreamID]struct{})
	new, errc := cm.coordinator.WatchTasks(cm.done)
	for {
		select {
		case <-cm.done:
			return nil
		case err := <-errc:
			log.Println("[error] managetasks", err)
			return err
		case task := <-cm.taskFailedCh:
			task.Stop()
		case candidateTasks := <-new:
			log.Println("got new tasks", candidateTasks)
			randomOrder := rand.Perm(len(candidateTasks))
			for _, i := range randomOrder {
				streamID := StreamID(candidateTasks[i])
				if _, alreadyTaken := cm.tasks[streamID]; alreadyTaken {
					continue
				}
				if _, found := unapplicableSet[streamID]; found {
					continue
				}
				gotTask, err := cm.coordinator.AcquireTask(streamID)
				if err != nil {
					// FIXME
					log.Println("[ERROR][coordinator]:", err)
					panic(err)
				}
				if gotTask {
					log.Println("[coordinator] Got task:", streamID)
					task, err := cm.setupTask(streamID)
					if err != nil {
						log.Println("Error setting up computation:", err) // FIXME
						cm.coordinator.ReleaseTask(streamID)              // FIXME ensure someone else tries to acquire
						unapplicableSet[streamID] = struct{}{}
						continue
					}
					cm.tasks[streamID] = task
					go func() {
						errc := make(chan error)
						task.Run(errc)
						if err := <-errc; err != nil {
							log.Println("[taskManager] task failed:", err)
							cm.taskFailedCh <- task
						}
					}()
					// job set up successfuly, register as publisher and delete the job
					log.Println("[coordinator] Deleting job: ", streamID)
					err = cm.coordinator.TaskAcquired(streamID)
					if err != nil {
						return err
					}
					err = cm.coordinator.RegisterAsPublisher(streamID)
					if err != nil {
						return err
					}
				}
			}
		}
	}
}

func (cm *TaskManager) setupTask(streamID StreamID) (Task, error) {
	name, definition, err := ParseComputationID(streamID)
	if err != nil {
		return nil, err
	}

	plugin, err := StartComputationPlugin(name, streamID)
	if err != nil {
		return nil, err
	}

	// get information about the plugin, such as which input streams it needs
	info, err := plugin.GetInfo(definition)
	if err != nil {
		plugin.Stop()
		return nil, err
	}

	var computation Task
	if info.Stateful {
		computation, err = newStatefulComputation(streamID, cm.coordinator, cm.persister, plugin)
		if err != nil {
			plugin.Stop()
			return nil, err
		}
	} else {
		computation = &statelessComputation{plugin, cm.dispatcher}
	}

	from, err := computation.Sync()
	if err != nil {
		plugin.Stop()
		return nil, err
	}

	for _, input := range info.Inputs {
		log.Println("subscribing to stream", input, "from", from)
		cm.receiver.SubscribeTo(input, from, computation)
	}
	return computation, nil
}

// GetSnapshot returns a snapshot of the requested task
func (cm *TaskManager) GetSnapshot(streamID StreamID) ([]byte, error) {
	comp, has := cm.tasks[streamID]
	if !has {
		return nil, fmt.Errorf("Computation not found!")
	}
	return comp.GetSnapshot()
}
