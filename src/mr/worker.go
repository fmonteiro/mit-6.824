package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// KeyValue example
type KeyValue struct {
	Key   string
	Value string
}

type byKey []KeyValue

// for sorting by key.
func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapTask(task *Reply, mapf func(string, string) []KeyValue) error {
	fmt.Println("[Worker] Starting map task...")

	// open file
	file, err := os.Open(task.FilePath)
	if err != nil {
		log.Fatalf("[Worker] cannot open %v", task.FilePath)
		return err
	}

	// read content
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[Worker] cannot read %v", task.FilePath)
		return err
	}

	file.Close()

	kva := mapf(task.FilePath, string(content))

	sort.Sort(byKey(kva))

	// write to temp files
	var files []*os.File
	var encoders []*json.Encoder

	// create the temp files
	for i := 0; i < task.NReduce; i++ {
		file, err := ioutil.TempFile("./", fmt.Sprintf("mr-%v-%v-", task.TaskNumber, i))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(file)
		encoders = append(encoders, enc)
		files = append(files, file)
	}

	for _, kv := range kva {
		fileNum := ihash(kv.Key) % task.NReduce
		err := encoders[fileNum].Encode(kv)
		if err != nil {
			fmt.Printf("[Worker] Error Encoding : %v", err.Error())
			return err
		}
	}

	// atomically rename files
	for i := 0; i < task.NReduce; i++ {
		err := os.Rename(files[i].Name(), fmt.Sprintf("./mr-%v-%v", task.TaskNumber, i))
		if err != nil {
			return err
		}
		files[i].Close()
	}

	return nil
}

func reduceTask(task *Reply, reducef func(string, []string) string) error {
	fmt.Println("[Worker] Starting reduce task...")

	var files []*os.File
	var decoders []*json.Decoder

	var outputFile, err = ioutil.TempFile("./", fmt.Sprintf("mr-out-%v-", task.TaskNumber))
	if err != nil {
		log.Fatal("[Worker] Error in reduce")
		return err
	}

	for i := 0; ; i++ {
		fmt.Printf("Opening file mr-%v-%v\n", i, task.TaskNumber)
		file, err := os.Open(fmt.Sprintf("./mr-%v-%v", i, task.TaskNumber))
		if err != nil {
			break
		}
		dec := json.NewDecoder(file)
		files = append(files, file)
		decoders = append(decoders, dec)
	}

	var intermediate []KeyValue
	for i := 0; i < len(files); i++ {
		for {
			var kv KeyValue
			if err := decoders[i].Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(byKey(intermediate))

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	for _, file := range files {
		file.Close()
	}
	os.Rename(outputFile.Name(), fmt.Sprintf("./mr-out-%v", task.TaskNumber))
	outputFile.Close()

	return nil
}

// Worker example
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	err := call("Master.ConnectWorker", &struct{}{}, &struct{}{})
	if err != nil {
		os.Exit(1)
	}

	for {
		reply := Reply{}
		fmt.Println("[Worker] Requesting...")

		err := call("Master.RequestNextTask", &struct{}{}, &reply)

		if err != nil {
			fmt.Println("[Worker] Error when requesting for task, shutting down...", err)
			os.Exit(1)
			return
		}

		switch reply.Error {
		case NoTasksAvailableError:
			fmt.Println("[Worker] No more tasks available, shutting down worker...")
			os.Exit(1)
			return
		case WaitForTaskError:
			fmt.Println("[Worker] Waiting for task...")
			time.Sleep(10 * time.Second)

			canShutdownReply := CanShutdownReply{}

			call("Master.CanShutdown", &struct{}{}, &canShutdownReply)

			fmt.Println("[Worker] Checking if worker can shutdown...")

			if canShutdownReply.CanShutdown {
				fmt.Println("[Worker] Worker shutting down...")
				os.Exit(1)
			}
		}

		fmt.Println("[Worker] Task received...")

		if reply.TaskPhase == MapPhase {
			err := mapTask(&reply, mapf)
			if err != nil {
				fmt.Println("[Worker] Error in map phase...", err)
				return
			}
		} else {
			err := reduceTask(&reply, reducef)
			if err != nil {
				fmt.Println("[Worker] Error in reduce phase...", err)
				return
			}
		}

		finishTaskInfo := FinishTaskInfo{TaskNumber: reply.TaskNumber, TaskFile: reply.FilePath}

		if err := call("Master.FinishTask", &finishTaskInfo, &struct{}{}); err != nil {
			fmt.Printf("[Worker] Task not finished %s", reply.FilePath)
		} else {
			fmt.Printf("\n[Worker] Task completed: %s\n", reply.FilePath)
		}

		canShutdownReply := CanShutdownReply{}

		call("Master.CanShutdown", &struct{}{}, &canShutdownReply)

		fmt.Println("[Worker] Checking if worker can shutdown...")

		if canShutdownReply.CanShutdown {
			fmt.Println("[Worker] Worker shutting down...")
			os.Exit(1)
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := masterSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	// if err != nil {
	// 	log.Fatal("dialing:", err)
	// }
	defer c.Close()

	err = c.Call(rpcname, args, reply)

	return err
}
