package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		args := TaskArgs{}
		reply := TaskReply{}
		if call("Master.AskForTask", &args, &reply) == false {
			break
		}

		// fmt.Println(reply.CurrTask)
		taskType, fileName, nReduce, taskId, reduceId := reply.CurrTask.TaskType, reply.CurrTask.Source, reply.CurrTask.NReduce, reply.CurrTask.TaskId, reply.CurrTask.ReduceId

		if taskType == "map" {
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open #{fileName}")
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read #{fileName}")
			}
			file.Close()
			kva := mapf(fileName, string(content))

			mapBucket := make(map[int][]KeyValue)
			for _, item := range kva {
				reduceId := ihash(item.Key) % nReduce
				mapBucket[reduceId] = append(mapBucket[reduceId], item)
			}

			for i := 0; i < nReduce; i++ {
				tmpfile, err := ioutil.TempFile("", "")
				if err != nil {
					log.Fatalf("Cannot create temp file")
				}
				enc := json.NewEncoder(tmpfile)
				for _, kv := range mapBucket[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("Cannot encode key-value pair")
					}
				}
				os.Rename(tmpfile.Name(), fmt.Sprintf("tmp-map-%v-%v", taskId, i))
				// fmt.Println(fmt.Sprintf("tmp-map-%v-%v", taskId, i))
				tmpfile.Close()
			}

		} else if taskType == "reduce" {
			mapTaskNum := 8

			kva := []KeyValue{}
			// read tmp_map_*_reduceId
			for i := 0; i < mapTaskNum; i++ {
				tmpMapFileName := fmt.Sprintf("tmp-map-%v-%v", i, reduceId)
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
					kva = append(kva, kv)
				}
				// fmt.Println(tmpMapFileName)
				// os.Remove(tmpMapFileName)
			}

			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%v", reduceId)
			tmpfile, err := ioutil.TempFile("", "")
			if err != nil {
				log.Fatalf("Cannot open temp file")
			}

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ { // sliding windows
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			os.Rename(tmpfile.Name(), oname)
			// fmt.Println(oname)
			tmpfile.Close()
		}
		args = TaskArgs{}
		args.CurrTask = reply.CurrTask

		if call("Master.FinishTask", &args, &reply) == false {
			break
		}
	}


}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
