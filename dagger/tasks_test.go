package dagger

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
)

func TestManageTasks(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	testTask := NewMockTask(mockCtrl)
	testTaskInfo := &TaskInfo{
		StreamID("t1"),
		testTask,
		[]StreamID{StreamID("s1"), StreamID("s2")},
		Timestamp(12345),
	}
	testFailingTask := NewMockTask(mockCtrl)
	testFailingTaskInfo := &TaskInfo{
		StreamID("t3"),
		testFailingTask,
		[]StreamID{StreamID("s3")},
		Timestamp(0),
	}
	taskStarter := NewMockTaskStarter(mockCtrl)

	inputManager := NewMockInputManager(mockCtrl)

	newTasks := make(chan []string)
	errc := make(chan error)
	coord := NewMockCoordinator(mockCtrl)

	inputManager.EXPECT().SetTaskManager(gomock.Any())
	tm := NewTaskManager(coord, inputManager, taskStarter)

	coord.EXPECT().WatchTasks(gomock.Any()).Return(newTasks, errc)
	go tm.ManageTasks()

	// starting a task fails
	coord.EXPECT().AcquireTask(StreamID("t1")).Return(true, nil)
	taskStarter.EXPECT().StartTask(StreamID("t1")).Return(nil, errors.New("testerr"))
	coord.EXPECT().ReleaseTask(StreamID("t1"))

	// task successfuly acquired and run
	coord.EXPECT().AcquireTask(StreamID("t2")).Return(true, nil)
	taskStarter.EXPECT().StartTask(StreamID("t2")).Return(testTaskInfo, nil)
	coord.EXPECT().TaskAcquired(StreamID("t2"))
	inputManager.EXPECT().SubscribeTo(StreamID("s1"), Timestamp(12345), testTask)
	inputManager.EXPECT().SubscribeTo(StreamID("s2"), Timestamp(12345), testTask)
	testTask.EXPECT().Run(gomock.Any())
	coord.EXPECT().RegisterAsPublisher(StreamID("t2"))

	// task successfuly acquired and run, but encounters an error
	coord.EXPECT().AcquireTask(StreamID("t3")).Return(true, nil)
	taskStarter.EXPECT().StartTask(StreamID("t3")).Return(testFailingTaskInfo, nil)
	coord.EXPECT().TaskAcquired(StreamID("t3"))
	inputManager.EXPECT().SubscribeTo(StreamID("s3"), Timestamp(0), testFailingTask)
	coord.EXPECT().RegisterAsPublisher(StreamID("t3"))
	testFailingTask.EXPECT().Stop()
	inputManager.EXPECT().UnsubscribeFrom(StreamID("s3"), testFailingTask)
	testFailingTask.EXPECT().Run(gomock.Any()).Do(func(errc chan error) {
		go func() { errc <- errors.New("task runtime error") }()
	})

	newTasks <- []string{"t1", "t2", "t3"}

	time.Sleep(100 * time.Millisecond)
	tm.Stop()
}
