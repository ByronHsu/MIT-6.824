package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	AvailTasks chan Task
	M int // number of reduce task
	Finish bool
	Tasks []Task
	N int // number of map task
	mux sync.Mutex
	Phase string
	NextPhase chan bool
}

type Task struct {
	TaskType string
	Source string
	NReduce int
	TaskId int
	ReduceId int
	Finish bool
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTaskGlobalId(task Task) int {
	if task.TaskType == "map" {
		return task.TaskId
	} else {
		return m.N + task.ReduceId
	}
}

func (m *Master) AskForTask(args *TaskArgs, reply *TaskReply) error {
	//fmt.Println(args, reply)
	//fmt.Println(m.Tasks)
	reply.CurrTask = <- m.AvailTasks
	m.mux.Lock()
	defer m.mux.Unlock()

	m.Tasks[m.GetTaskGlobalId(reply.CurrTask)].StartTime = time.Now()
	return nil
}

func (m *Master) FinishTask(args *TaskArgs, reply *TaskReply) error {
	//fmt.Println(args, reply)
	m.mux.Lock()
	defer m.mux.Unlock()

	m.Tasks[m.GetTaskGlobalId(args.CurrTask)].Finish = true
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
	if m.Finish == true {
		ret = true
	}

	return ret
}


func (m *Master) Probe() {
	// 1. check whether current phase is finished (finished_map == N or finished_reduce == M)
	// 2. assign timeout task back to availTask
	nFinishMap := 0
	nFinishReduce := 0

	for _, task := range m.Tasks {
		if time.Now().Sub(task.StartTime).Seconds() > 10 { // timeout
			// put it into avail task channel again
			m.AvailTasks <- task
		}
	}

	for _, task := range m.Tasks {
		if task.TaskType == "map" && task.Finish == true {
			nFinishMap++
		}
		if task.TaskType == "reduce" && task.Finish == true {
			nFinishReduce++
		}
	}
	//fmt.Println("nFinishMap", nFinishMap)
	//fmt.Println("numOfMap", m.N)
	//fmt.Println("phase", m.Phase)

	if m.Phase == "map" && nFinishMap == m.N {
		m.Phase = "reduce"
		//fmt.Println("here")
		m.NextPhase <- true

	}
	if m.Phase == "reduce" && nFinishReduce == m.M {
		m.NextPhase <- true
	}
}
//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{AvailTasks: make(chan Task), N: len(files),  M: nReduce, Finish: false, Phase: "map", NextPhase: make(chan bool)}

	// Your code here.

	m.server()


	// iteratively track each task's status
	// if the task has run over 10s, put the task back to available task channel
	go func() {
		for {
			m.Probe()
			time.Sleep(1000 * time.Millisecond)
		}
	}()


	for i, filename := range files {
		newTask := Task{TaskType: "map", Source: filename, NReduce: nReduce, TaskId: i, Finish: false}
		m.Tasks = append(m.Tasks, newTask)
		m.AvailTasks <- newTask
	}
	//fmt.Println("hehe")
	<- m.NextPhase
	//fmt.Println("herehere")
	for i := 0; i < nReduce; i++ {
		newTask := Task{TaskType: "reduce", ReduceId: i, Finish: false}
		m.Tasks = append(m.Tasks, newTask)
		m.AvailTasks <- newTask
	}
	<- m.NextPhase

	m.Finish = true
	

	return &m
}
