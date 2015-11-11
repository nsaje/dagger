package dagger

type Task string

type TaskManager interface {
	ManageTasks()
}
