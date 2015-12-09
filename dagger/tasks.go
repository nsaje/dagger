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
	ManageTasks() error
	GetTaskSnapshot(StreamID) ([]byte, error)
	Stop()
}

type taskManager struct {
	tasks        map[StreamID]Task
	coordinator  Coordinator
	receiver     InputManager
	taskStarter  TaskStarter
	done         chan struct{}
	taskFailedCh chan *TaskInfo

	counter metrics.Counter
}

// NewTaskManager creates a new task manager
func NewTaskManager(coordinator Coordinator, receiver InputManager, taskStarter TaskStarter) TaskManager {
	c := metrics.NewCounter()
	metrics.Register("processing", c)
	tm := &taskManager{
		tasks:        make(map[StreamID]Task),
		coordinator:  coordinator,
		receiver:     receiver,
		taskStarter:  taskStarter,
		counter:      c,
		done:         make(chan struct{}),
		taskFailedCh: make(chan *TaskInfo),
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

func (cm *taskManager) Stop() {
	cm.done <- struct{}{}
	<-cm.done
}

// ManageTasks watches for new tasks and tries to acquire and run them
func (cm *taskManager) ManageTasks() error {
	unapplicableSet := make(map[StreamID]struct{})
	new, errc := cm.coordinator.WatchTasks(cm.done)
	for {
		select {
		case <-cm.done:
			cm.done <- struct{}{}
			return nil
		case err := <-errc:
			log.Println("[error][taskManager] managetasks", err)
			return err
		case taskInfo := <-cm.taskFailedCh:
			log.Println("[taskManager] stopping failed task", taskInfo.StreamID)
			taskInfo.Task.Stop()
			delete(cm.tasks, taskInfo.StreamID)
			for _, input := range taskInfo.Inputs {
				cm.receiver.UnsubscribeFrom(input, taskInfo.Task)
			}
		case candidateTasks, ok := <-new:
			if !ok {
				return nil
			}
			log.Println("[taskManager] got new tasks", candidateTasks)

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
					name, definition, err := ParseComputationID(streamID)
					if err != nil {
						unapplicableSet[streamID] = struct{}{}
						continue
					}
					var taskInfo *TaskInfo
					if name == "match" {
						// internal task
						taskInfo, err = NewMatchTask(cm.coordinator, cm.receiver, streamID, definition)
					} else {
						taskInfo, err = cm.taskStarter.StartTask(streamID, name, definition)
					}
					if err != nil {
						log.Println("Error setting up computation:", err) // FIXME
						cm.coordinator.ReleaseTask(streamID)              // FIXME ensure someone else tries to acquire
						unapplicableSet[streamID] = struct{}{}
						continue
					}
					for _, input := range taskInfo.Inputs {
						cm.receiver.SubscribeTo(input, taskInfo.From, taskInfo.Task)
					}
					cm.tasks[streamID] = taskInfo.Task
					go func() {
						errc := make(chan error)
						taskInfo.Task.Run(errc)
						if err := <-errc; err != nil {
							log.Println("[taskManager] task failed:", err)
							cm.taskFailedCh <- taskInfo
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
	StreamID StreamID
	Task     Task
	Inputs   []StreamID
	From     Timestamp
}

// TaskStarter sets up a task from a given stream ID
type TaskStarter interface {
	StartTask(StreamID, string, string) (*TaskInfo, error)
}

func NewTaskStarter(c Coordinator, p Persister, dispatcherConfig func(*DispatcherConfig)) TaskStarter {
	return &defaultTaskStarter{c, p, dispatcherConfig}
}

type defaultTaskStarter struct {
	coordinator      Coordinator
	persister        Persister
	dispatcherConfig func(*DispatcherConfig)
}

func (cm *defaultTaskStarter) StartTask(streamID StreamID, name string, definition string) (*TaskInfo, error) {
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
		task, err = newStatefulComputation(streamID, cm.coordinator, cm.persister, plugin, cm.dispatcherConfig)
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

	return &TaskInfo{streamID, task, info.Inputs, from}, nil
}
