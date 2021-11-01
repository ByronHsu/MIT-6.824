package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	AvailTask chan Task
	NReduce int
	Finish bool
}

type Task struct {
	TaskType string
	Source string
	NReduce int
	TaskId int
	ReduceId int
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

func (m *Master) AskForTask(args *TaskArgs, reply *TaskReply) error {
	reply.CurrTask = <- m.AvailTask
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

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{AvailTask: make(chan Task), NReduce: nReduce, Finish: false}

	// Your code here.

	m.server()

	// the first one in files is wc.so, so we truncate the file list
	for i, filename := range files {
		m.AvailTask <- Task{TaskType: "map", Source: filename, NReduce: nReduce, TaskId: i}
	}

	for i := 0; i < nReduce; i++ {
		m.AvailTask <- Task{TaskType: "reduce", ReduceId: i}
	}

	m.Finish = true


	return &m
}
