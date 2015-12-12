// Automatically generated by MockGen. DO NOT EDIT!
// Source: github.com/nsaje/dagger/dagger (interfaces: Coordinator,InputManager,TaskStarter,Task)

package dagger

import (
	net "net"

	gomock "github.com/golang/mock/gomock"
)

// Mock of Coordinator interface
type MockCoordinator struct {
	ctrl     *gomock.Controller
	recorder *_MockCoordinatorRecorder
}

// Recorder for MockCoordinator (not exported)
type _MockCoordinatorRecorder struct {
	mock *MockCoordinator
}

func NewMockCoordinator(ctrl *gomock.Controller) *MockCoordinator {
	mock := &MockCoordinator{ctrl: ctrl}
	mock.recorder = &_MockCoordinatorRecorder{mock}
	return mock
}

func (_m *MockCoordinator) EXPECT() *_MockCoordinatorRecorder {
	return _m.recorder
}

func (_m *MockCoordinator) AcquireTask(_param0 StreamID) (bool, error) {
	ret := _m.ctrl.Call(_m, "AcquireTask", _param0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCoordinatorRecorder) AcquireTask(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "AcquireTask", arg0)
}

func (_m *MockCoordinator) CheckpointPosition(_param0 StreamID, _param1 Timestamp) error {
	ret := _m.ctrl.Call(_m, "CheckpointPosition", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCoordinatorRecorder) CheckpointPosition(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CheckpointPosition", arg0, arg1)
}

func (_m *MockCoordinator) DeregisterAsPublisher(_param0 StreamID) error {
	ret := _m.ctrl.Call(_m, "DeregisterAsPublisher", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCoordinatorRecorder) DeregisterAsPublisher(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "DeregisterAsPublisher", arg0)
}

func (_m *MockCoordinator) EnsurePublisherNum(_param0 StreamID, _param1 int, _param2 chan struct{}) chan error {
	ret := _m.ctrl.Call(_m, "EnsurePublisherNum", _param0, _param1, _param2)
	ret0, _ := ret[0].(chan error)
	return ret0
}

func (_mr *_MockCoordinatorRecorder) EnsurePublisherNum(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "EnsurePublisherNum", arg0, arg1, arg2)
}

func (_m *MockCoordinator) GetSubscriberPosition(_param0 StreamID, _param1 string) (Timestamp, error) {
	ret := _m.ctrl.Call(_m, "GetSubscriberPosition", _param0, _param1)
	ret0, _ := ret[0].(Timestamp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCoordinatorRecorder) GetSubscriberPosition(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetSubscriberPosition", arg0, arg1)
}

func (_m *MockCoordinator) GetSubscribers(_param0 StreamID) ([]string, error) {
	ret := _m.ctrl.Call(_m, "GetSubscribers", _param0)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCoordinatorRecorder) GetSubscribers(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetSubscribers", arg0)
}

func (_m *MockCoordinator) JoinGroup(_param0 StreamID) (GroupHandler, error) {
	ret := _m.ctrl.Call(_m, "JoinGroup", _param0)
	ret0, _ := ret[0].(GroupHandler)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCoordinatorRecorder) JoinGroup(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "JoinGroup", arg0)
}

func (_m *MockCoordinator) RegisterAsPublisher(_param0 StreamID) error {
	ret := _m.ctrl.Call(_m, "RegisterAsPublisher", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCoordinatorRecorder) RegisterAsPublisher(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "RegisterAsPublisher", arg0)
}

func (_m *MockCoordinator) RegisterAsPublisherWithSession(_param0 string, _param1 StreamID) error {
	ret := _m.ctrl.Call(_m, "RegisterAsPublisherWithSession", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCoordinatorRecorder) RegisterAsPublisherWithSession(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "RegisterAsPublisherWithSession", arg0, arg1)
}

func (_m *MockCoordinator) RegisterSession() (string, error) {
	ret := _m.ctrl.Call(_m, "RegisterSession")
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCoordinatorRecorder) RegisterSession() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "RegisterSession")
}

func (_m *MockCoordinator) ReleaseTask(_param0 StreamID) (bool, error) {
	ret := _m.ctrl.Call(_m, "ReleaseTask", _param0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCoordinatorRecorder) ReleaseTask(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ReleaseTask", arg0)
}

func (_m *MockCoordinator) RenewSession(_param0 string) error {
	ret := _m.ctrl.Call(_m, "RenewSession", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCoordinatorRecorder) RenewSession(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "RenewSession", arg0)
}

func (_m *MockCoordinator) Start(_param0 net.Addr, _param1 chan error) error {
	ret := _m.ctrl.Call(_m, "Start", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCoordinatorRecorder) Start(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Start", arg0, arg1)
}

func (_m *MockCoordinator) Stop() {
	_m.ctrl.Call(_m, "Stop")
}

func (_mr *_MockCoordinatorRecorder) Stop() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Stop")
}

func (_m *MockCoordinator) SubscribeTo(_param0 StreamID, _param1 Timestamp) error {
	ret := _m.ctrl.Call(_m, "SubscribeTo", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCoordinatorRecorder) SubscribeTo(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SubscribeTo", arg0, arg1)
}

func (_m *MockCoordinator) TaskAcquired(_param0 StreamID) error {
	ret := _m.ctrl.Call(_m, "TaskAcquired", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCoordinatorRecorder) TaskAcquired(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "TaskAcquired", arg0)
}

func (_m *MockCoordinator) UnsubscribeFrom(_param0 StreamID) error {
	ret := _m.ctrl.Call(_m, "UnsubscribeFrom", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCoordinatorRecorder) UnsubscribeFrom(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "UnsubscribeFrom", arg0)
}

func (_m *MockCoordinator) WatchSubscriberPosition(_param0 StreamID, _param1 string, _param2 chan struct{}) (chan Timestamp, chan error) {
	ret := _m.ctrl.Call(_m, "WatchSubscriberPosition", _param0, _param1, _param2)
	ret0, _ := ret[0].(chan Timestamp)
	ret1, _ := ret[1].(chan error)
	return ret0, ret1
}

func (_mr *_MockCoordinatorRecorder) WatchSubscriberPosition(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "WatchSubscriberPosition", arg0, arg1, arg2)
}

func (_m *MockCoordinator) WatchSubscribers(_param0 StreamID, _param1 chan struct{}) (chan string, chan string, chan error) {
	ret := _m.ctrl.Call(_m, "WatchSubscribers", _param0, _param1)
	ret0, _ := ret[0].(chan string)
	ret1, _ := ret[1].(chan string)
	ret2, _ := ret[2].(chan error)
	return ret0, ret1, ret2
}

func (_mr *_MockCoordinatorRecorder) WatchSubscribers(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "WatchSubscribers", arg0, arg1)
}

func (_m *MockCoordinator) WatchTagMatch(_param0 StreamID, _param1 chan string, _param2 chan string, _param3 chan error) {
	_m.ctrl.Call(_m, "WatchTagMatch", _param0, _param1, _param2, _param3)
}

func (_mr *_MockCoordinatorRecorder) WatchTagMatch(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "WatchTagMatch", arg0, arg1, arg2, arg3)
}

func (_m *MockCoordinator) WatchTasks(_param0 chan struct{}) (chan []string, chan error) {
	ret := _m.ctrl.Call(_m, "WatchTasks", _param0)
	ret0, _ := ret[0].(chan []string)
	ret1, _ := ret[1].(chan error)
	return ret0, ret1
}

func (_mr *_MockCoordinatorRecorder) WatchTasks(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "WatchTasks", arg0)
}

// Mock of InputManager interface
type MockInputManager struct {
	ctrl     *gomock.Controller
	recorder *_MockInputManagerRecorder
}

// Recorder for MockInputManager (not exported)
type _MockInputManagerRecorder struct {
	mock *MockInputManager
}

func NewMockInputManager(ctrl *gomock.Controller) *MockInputManager {
	mock := &MockInputManager{ctrl: ctrl}
	mock.recorder = &_MockInputManagerRecorder{mock}
	return mock
}

func (_m *MockInputManager) EXPECT() *_MockInputManagerRecorder {
	return _m.recorder
}

func (_m *MockInputManager) SetTaskManager(_param0 TaskManager) {
	_m.ctrl.Call(_m, "SetTaskManager", _param0)
}

func (_mr *_MockInputManagerRecorder) SetTaskManager(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetTaskManager", arg0)
}

func (_m *MockInputManager) SubscribeTo(_param0 StreamID, _param1 Timestamp, _param2 RecordProcessor) {
	_m.ctrl.Call(_m, "SubscribeTo", _param0, _param1, _param2)
}

func (_mr *_MockInputManagerRecorder) SubscribeTo(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SubscribeTo", arg0, arg1, arg2)
}

func (_m *MockInputManager) UnsubscribeFrom(_param0 StreamID, _param1 RecordProcessor) {
	_m.ctrl.Call(_m, "UnsubscribeFrom", _param0, _param1)
}

func (_mr *_MockInputManagerRecorder) UnsubscribeFrom(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "UnsubscribeFrom", arg0, arg1)
}

// Mock of TaskStarter interface
type MockTaskStarter struct {
	ctrl     *gomock.Controller
	recorder *_MockTaskStarterRecorder
}

// Recorder for MockTaskStarter (not exported)
type _MockTaskStarterRecorder struct {
	mock *MockTaskStarter
}

func NewMockTaskStarter(ctrl *gomock.Controller) *MockTaskStarter {
	mock := &MockTaskStarter{ctrl: ctrl}
	mock.recorder = &_MockTaskStarterRecorder{mock}
	return mock
}

func (_m *MockTaskStarter) EXPECT() *_MockTaskStarterRecorder {
	return _m.recorder
}

func (_m *MockTaskStarter) StartTask(_param0 StreamID, _param1 string, _param2 string) (*TaskInfo, error) {
	ret := _m.ctrl.Call(_m, "StartTask", _param0, _param1, _param2)
	ret0, _ := ret[0].(*TaskInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockTaskStarterRecorder) StartTask(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "StartTask", arg0, arg1, arg2)
}

// Mock of Task interface
type MockTask struct {
	ctrl     *gomock.Controller
	recorder *_MockTaskRecorder
}

// Recorder for MockTask (not exported)
type _MockTaskRecorder struct {
	mock *MockTask
}

func NewMockTask(ctrl *gomock.Controller) *MockTask {
	mock := &MockTask{ctrl: ctrl}
	mock.recorder = &_MockTaskRecorder{mock}
	return mock
}

func (_m *MockTask) EXPECT() *_MockTaskRecorder {
	return _m.recorder
}

func (_m *MockTask) GetSnapshot() ([]byte, error) {
	ret := _m.ctrl.Call(_m, "GetSnapshot")
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockTaskRecorder) GetSnapshot() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetSnapshot")
}

func (_m *MockTask) ProcessRecord(_param0 *Record) error {
	ret := _m.ctrl.Call(_m, "ProcessRecord", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockTaskRecorder) ProcessRecord(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ProcessRecord", arg0)
}

func (_m *MockTask) Run(_param0 chan error) {
	_m.ctrl.Call(_m, "Run", _param0)
}

func (_mr *_MockTaskRecorder) Run(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Run", arg0)
}

func (_m *MockTask) Stop() {
	_m.ctrl.Call(_m, "Stop")
}

func (_mr *_MockTaskRecorder) Stop() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Stop")
}

func (_m *MockTask) Sync() (Timestamp, error) {
	ret := _m.ctrl.Call(_m, "Sync")
	ret0, _ := ret[0].(Timestamp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockTaskRecorder) Sync() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Sync")
}
