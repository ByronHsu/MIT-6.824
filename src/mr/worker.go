package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		args := AskForTaskArgs{}
		reply := AskForTaskReply{}
		if call("Master.AskForTask", &args, &reply) == false {
			break
		}

		_type, mapFileName, nMap, nReduce, taskId := reply.Type, reply.MapFileName, reply.NMap, reply.NReduce, reply.TaskId

		if _type == MAP {
			file, err := os.Open(mapFileName)
			if err != nil {
				log.Fatalf("cannot open #{fileName}")
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read #{fileName}")
			}
			file.Close()
			kva := mapf(mapFileName, string(content))

			mapBucket := make(map[int][]KeyValue)
			for _, item := range kva {
				reduceId := ihash(item.Key) % nReduce
				mapBucket[reduceId] = append(mapBucket[reduceId], item)
			}

			for i := 0; i < nReduce; i++ {
				tmpFile, err := ioutil.TempFile("", "")
				oName := GetMapTempName(taskId, strconv.Itoa(i))

				if err != nil {
					log.Fatalf("Cannot create temp file")
				}
				enc := json.NewEncoder(tmpFile)
				for _, kv := range mapBucket[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("Cannot encode key-value pair")
					}
				}
				os.Rename(tmpFile.Name(), oName)
				// fmt.Println(oName)
				tmpFile.Close()
			}

		} else if _type == REDUCE {
			mapkv := map[string][]string{}

			for i := 0; i < nMap; i++ {
				tmpMapFileName := GetMapTempName(strconv.Itoa(i), taskId)

				file, err := os.Open(tmpMapFileName)
				if err != nil {
					log.Fatalf("Cannot open temp file")
				}
				
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					mapkv[kv.Key] = append(mapkv[kv.Key], kv.Value)
				}
			}

			oName := GetOutputName(taskId)
			tmpFile, err := ioutil.TempFile("", "")

			if err != nil {
				log.Fatalf("Cannot open temp file")
			}

			for key, values := range mapkv {
				output := reducef(key, values)
				fmt.Fprintf(tmpFile, "%v %v\n", key, output)
			}

			os.Rename(tmpFile.Name(), oName)
			tmpFile.Close()
		}

		args2 := NotifyTaskFinishedArgs{Type: _type, TaskId: taskId}
		reply2 := NotifyTaskFinishedReply{Done: false}

		if call("Master.NotifyTaskFinished", &args2, &reply2) == false {
			break
		}
	}


}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
