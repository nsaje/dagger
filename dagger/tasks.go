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
	GetSnapshot() ([]byte, error)
	Sync() (Timestamp, error)
	Run(chan error)
	Stop()
}

// TaskManager manages tasks
type TaskManager interface {
	ManageTasks(TaskStarter) error
	GetTaskSnapshot(StreamID) ([]byte, error)
}

type taskManager struct {
	tasks        map[StreamID]Task
	coordinator  Coordinator
	receiver     InputManager
	done         chan struct{}
	taskFailedCh chan Task

	counter metrics.Counter
}

// NewTaskManager creates a new task manager
func NewTaskManager(coordinator Coordinator, receiver InputManager, persister Persister) TaskManager {
	c := metrics.NewCounter()
	metrics.Register("processing", c)
	tm := &taskManager{
		tasks:       make(map[StreamID]Task),
		coordinator: coordinator,
		receiver:    receiver,
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
func (cm *taskManager) ManageTasks(taskStarter TaskStarter) error {
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

			// try to acquire available tasks in random order, so the tasks are
			// spread evenly among workers
			randomOrder := rand.Perm(len(candidateTasks))
			for _, i := range randomOrder {
				streamID := StreamID(candidateTasks[i])
				if _, alreadyTaken := cm.tasks[streamID]; alreadyTaken {
					continue
				}
				// have we already tried and failed to start this task?
				if _, found := unapplicableSet[streamID]; found {
					continue
				}
				gotTask, err := cm.coordinator.AcquireTask(streamID)
				if err != nil {
					log.Println("[ERROR][coordinator]:", err)
					continue
				}
				if gotTask {
					log.Println("[coordinator] Got task:", streamID)
					taskInfo, err := taskStarter.StartTask(streamID)
					if err != nil {
						log.Println("Error setting up computation:", err) // FIXME
						cm.coordinator.ReleaseTask(streamID)              // FIXME ensure someone else tries to acquire
						unapplicableSet[streamID] = struct{}{}
						continue
					}
					task := taskInfo.Task
					for _, input := range taskInfo.Inputs {
						log.Println("subscribing to stream", input, "from", taskInfo.From)
						cm.receiver.SubscribeTo(input, taskInfo.From, task)
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

// GetTaskSnapshot returns a snapshot of the requested task
func (cm *taskManager) GetTaskSnapshot(streamID StreamID) ([]byte, error) {
	comp, has := cm.tasks[streamID]
	if !has {
		return nil, fmt.Errorf("Computation not found!")
	}
	return comp.GetSnapshot()
}

// TaskInfo summarizes the newly created task with info about its input
// and position up to which it has already processed data
type TaskInfo struct {
	Task   Task
	Inputs []StreamID
	From   Timestamp
}

// TaskStarter sets up a task from a given stream ID
type TaskStarter interface {
	StartTask(StreamID) (*TaskInfo, error)
}

type DefaultStarter struct {
	coordinator Coordinator
	persister   Persister
}

func (cm *DefaultStarter) StartTask(streamID StreamID) (*TaskInfo, error) {
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

	var task Task
	if info.Stateful {
		task, err = newStatefulComputation(streamID, cm.coordinator, cm.persister, plugin)
		if err != nil {
			plugin.Stop()
			return nil, err
		}
	} else {
		// in the future different types of tasks will be set up here
		plugin.Stop()
		return nil, fmt.Errorf("Unsupported task type!")
	}

	from, err := task.Sync()
	if err != nil {
		plugin.Stop()
		return nil, err
	}

	return &TaskInfo{task, info.Inputs, from}, nil
}
