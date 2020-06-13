package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

// Task - representation of a task
type Task struct {
	file       string
	taskNumber int
	taskPhase  TaskPhase
}

// Master just a struct
type Master struct {
	taskList        []Task
	inProgressTasks []Task
	doneTasks       []Task
	numTasks        int
	numWorkers      int

	nReduce int
	phase   TaskPhase

	mux sync.Mutex
}

// ConnectWorker - connect the worker to the master
func (m *Master) ConnectWorker(_ *ExampleArgs, _ *ExampleReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	m.numWorkers++
	return nil
}

// RequestNextTask - returns the next task for the worker
func (m *Master) RequestNextTask(_ *ExampleArgs, reply *Reply) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	fmt.Println("[Master] Received request...")

	if m.isComplete() {
		fmt.Println("[Master] All tasks completed...")
		reply.Error = NoTasksAvailableError
		return nil
	}

	if len(m.taskList) == 0 {
		fmt.Println("[Master] No more tasks for the moment...")
		reply.Error = WaitForTaskError
		return nil
	}

	nextTask := m.taskList[len(m.taskList)-1]

	fmt.Printf("[Master] Assigning next task %s...\n", nextTask.file)

	// add task to in progress
	m.inProgressTasks = append(m.inProgressTasks, nextTask)
	// remove task from task list
	m.taskList = m.taskList[:len(m.taskList)-1]

	reply.NReduce = m.nReduce
	reply.FilePath = nextTask.file
	reply.TaskNumber = nextTask.taskNumber
	reply.TaskPhase = nextTask.taskPhase
	reply.Error = NoError

	go m.monitorWorker(nextTask)

	fmt.Printf("[Master] Assignment completed, task %s...", reply.FilePath)

	return nil
}

// CanShutdown - tells the worker if it can shutdown
func (m *Master) CanShutdown(_ *ExampleArgs, reply *CanShutdownReply) error {
	fmt.Println("[Master] Checking if worker can shutdown...")

	m.mux.Lock()
	defer m.mux.Unlock()

	reply.CanShutdown = m.isComplete()

	if reply.CanShutdown {
		m.numWorkers--
	}

	return nil
}

// FinishTask - finish task
func (m *Master) FinishTask(finishTaskInfo *FinishTaskInfo, reply *ExampleReply) error {
	fmt.Printf("\n[Master] Checking if task %s can be finished...", finishTaskInfo.TaskFile)

	m.mux.Lock()
	defer m.mux.Unlock()

	// if the task is not in the task list and is in the progess list, can finish task
	task, err := getTaskByNumber(finishTaskInfo.TaskNumber, m.inProgressTasks)
	if err != nil {
		fmt.Printf("\n[Master] Cannot finish task %d...", finishTaskInfo.TaskNumber)
		return errors.New("[Master] Cannot finish task")
	}

	m.inProgressTasks = removeTask(task.taskNumber, m.inProgressTasks)
	m.doneTasks = append(m.doneTasks, task)

	fmt.Println("[Master] Task finished with success...")

	return nil
}

// Monitor the worker responsible for the task
// If the task is not done in 10 sec, it will assume
// the worker has problems
func (m *Master) monitorWorker(task Task) {
	fmt.Printf("\n[Master] Monitoring worker with task %d...", task.taskNumber)
	time.Sleep(10 * time.Second)

	m.mux.Lock()
	defer m.mux.Unlock()

	task, err := getTaskByNumber(task.taskNumber, m.inProgressTasks)

	if err != nil {
		fmt.Printf("\n[Master] Worker with task %d finished task during the time", task.taskNumber)
		return
	}

	fmt.Printf("\n[Master] Worker with task %d reached timeout", task.taskNumber)

	m.inProgressTasks = removeTask(task.taskNumber, m.inProgressTasks)
	m.taskList = append(m.taskList, task)
}

func (m *Master) changePhase() {
	for {
		fmt.Println("[Master] Checking task phase...")
		m.mux.Lock()

		shouldChangePhase := len(m.taskList) == 0 && len(m.inProgressTasks) == 0 && len(m.doneTasks) == m.numTasks

		if shouldChangePhase {
			fmt.Println("[Master] Switching task phase...")

			m.taskList = make([]Task, 0)

			for i := 0; i < m.nReduce; i++ {
				m.taskList = append(m.taskList,
					Task{taskPhase: ReducePhase, file: "", taskNumber: i})
			}

			m.inProgressTasks = make([]Task, 0)
			m.doneTasks = make([]Task, 0)

			m.phase = ReducePhase

			m.mux.Unlock()
			return
		}

		m.mux.Unlock()
		time.Sleep(2 * time.Second)
	}
}

// Not thread safe. Be aware
func getTaskByNumber(taskNumber int, tasks []Task) (Task, error) {
	for _, task := range tasks {
		if task.taskNumber == taskNumber {
			return task, nil
		}
	}
	return Task{}, errors.New("No tasks with that number")
}

// Not thread safe. Be aware
func removeTask(taskNumber int, tasks []Task) []Task {
	newTasks := make([]Task, 0)
	for _, task := range tasks {
		if task.taskNumber != taskNumber {
			newTasks = append(newTasks, task)
		}
	}

	return newTasks
}

func (m *Master) isComplete() bool {
	return len(m.taskList) == 0 && len(m.inProgressTasks) == 0 && m.phase == ReducePhase
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := masterSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//

// Done finish job
func (m *Master) Done() bool {
	m.mux.Lock()
	defer m.mux.Unlock()

	shouldExit := m.isComplete() && m.numWorkers == 0

	return shouldExit
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

// MakeMaster called by mrmaster
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:         nReduce,
		numTasks:        len(files),
		numWorkers:      0,
		phase:           MapPhase,
		taskList:        make([]Task, 0),
		inProgressTasks: make([]Task, 0),
		doneTasks:       make([]Task, 0),
	}

	for i, file := range files {
		m.taskList = append(
			m.taskList,
			Task{file: file, taskNumber: i, taskPhase: MapPhase},
		)
	}

	go m.changePhase()

	m.server()
	return &m
}
