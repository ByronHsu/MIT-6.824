package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	AvailTasks chan Task // queuing available tasks
	NMap int // number of map task
	NReduce int // number of reduce task
	RunningTasks map[string]Task // all tasks that is running. key is `#{TaskType}-#{TaskId`
	NextPhase chan bool // to block the availTasks from getting task in the next phase
	Mux sync.Mutex
	IsDone bool // whether the whole task finished
	NFinishedTasks int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	currTask := <- m.AvailTasks
	reply.Type = currTask.Type
	reply.MapFileName = currTask.MapFileName
	reply.TaskId = currTask.Id
	reply.NMap = m.NMap
	reply.NReduce = m.NReduce

	m.Mux.Lock() // protect shared data

	// https://stackoverflow.com/questions/32751537/why-do-i-get-a-cannot-assign-error-when-setting-value-to-a-struct-as-a-value-i
	// map[key] is not addressable. we should create a new task instance
	// m.RunningTasks[GetTaskName(currTask.Type, currTask.Id)].StartTime = time.Now() // TODO
	currTask.StartTime = time.Now()
	// put running task into map
	m.RunningTasks[GetTaskName(currTask.Type, currTask.Id)] = currTask

	defer m.Mux.Unlock()

	return nil
}

func (m *Master) NotifyTaskFinished(args *NotifyTaskFinishedArgs, reply *NotifyTaskFinishedReply) error {
	m.Mux.Lock()
	// delete the task, ie the task is finished
	delete(m.RunningTasks, GetTaskName(args.Type, args.TaskId))

	m.NFinishedTasks += 1

	if m.NFinishedTasks == m.NMap {
		m.NextPhase <- true
	} else if m.NFinishedTasks == m.NMap + m.NReduce { // all the tasks are finished
		m.IsDone = true
	}

	defer m.Mux.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.IsDone == true {
		ret = true
	}

	return ret
}


func Probe(m *Master)  {
	// iteratively track each task's status
	// if the task has run over 10s, put the task back to available task channel

	for {
		time.Sleep(1000 * time.Millisecond) // execute per 1s

		// assign timeout task back to availTask
		for _, task := range m.RunningTasks {
			// fmt.Println("task", task)
			if time.Now().Sub(task.StartTime).Seconds() > 10 { // timeout
				// put it into avail task channel again
				m.AvailTasks <- task
			}
		}
	}
}

func Execute(m *Master, files []string) {
	
	for i, filename := range files {
		newTask := Task{Type: MAP, MapFileName: filename, Id: strconv.Itoa(i)}
		m.AvailTasks <- newTask
	}

	<- m.NextPhase

	for i := 0; i < m.NReduce; i++ {
		newTask := Task{Type: REDUCE, Id: strconv.Itoa(i)}
		m.AvailTasks <- newTask
	}

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{AvailTasks: make(chan Task), NMap: len(files), NReduce: nReduce, RunningTasks: make(map[string]Task), NextPhase: make(chan bool), IsDone: false, NFinishedTasks: 0}

	m.server()

	go Probe(&m)

	Execute(&m, files)

	return &m
}
