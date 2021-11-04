package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"time"
)
import "strconv"

// enum for task type
type TaskType int8
const (
	MAP TaskType = iota
	REDUCE
)

type Task struct {
	Type TaskType
	MapFileName string
	Id string
	StartTime time.Time
}

type AskForTaskArgs struct {

}

type AskForTaskReply struct {
	Type TaskType
	MapFileName string
	NMap int
	NReduce int
	TaskId string
}

type NotifyTaskFinishedArgs struct {
	Type TaskType
	TaskId string
}

type NotifyTaskFinishedReply struct {
	Done bool
}

// utils function

func GetTaskName(taskType TaskType, taskId string) string {
	// to generate the task name used in RunningTasks
	return fmt.Sprintf("task-%v-%v", taskType, taskId)
}

func GetMapTempName(mapId string, reduceId string) string {
	return fmt.Sprintf("mr-%v-%v", mapId, reduceId)
}

func GetOutputName(reduceId string) string {
	return fmt.Sprintf("mr-out-%v", reduceId)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
