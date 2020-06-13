package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// ExampleArgs its an example
type ExampleArgs struct {
	X int
}

// ExampleReply its an example
type ExampleReply struct {
	Y     int
	Text  string
	Tasks []string
}

// Reply - reply
type Reply struct {
	NReduce    int
	FilePath   string
	TaskNumber int
	TaskPhase  TaskPhase
	Error      TaskError
}

// FinishTaskInfo - info about the task
type FinishTaskInfo struct {
	TaskNumber int
	TaskFile   string
}

// CanShutdownReply - info about can shutdown
type CanShutdownReply struct {
	CanShutdown bool
}

// Error types

// TaskError - custom error when dealing with tasks
type TaskError int

const (
	// NoTasksAvailable - error when there are no more tasks
	NoTasksAvailableError TaskError = 0
	// WaitForTask - error when there are no more tasks currently
	WaitForTaskError TaskError = 1
	// NoError - no error
	NoError TaskError = 2
)

// // NoTasksAvailable - error when there are no more tasks
// type NoTasksAvailable struct {
// 	message string
// }

// func (e *NoTasksAvailable) Error() string {
// 	return e.message
// }

// // WaitForTask - error when there are no more tasks currently
// type WaitForTask struct {
// 	message string
// }

// func (e *WaitForTask) Error() string {
// 	return e.message
// }

// // Task reply type
// type Task struct {
// 	FilePath   string
// 	TaskNumber int
// 	NReduce    int
// 	// map or reduce
// 	TaskType string
// }

// TaskPhase - phase of the tasks
type TaskPhase string

const (
	// MapPhase - map phase
	MapPhase TaskPhase = "Map"
	// ReducePhase - reduce phase
	ReducePhase = "Reduce"
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
