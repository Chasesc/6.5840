package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFile(filename string) (string, error) {
	file, err := os.Open(string(filename))
	if err != nil {
		return "", err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Each worker process will, in a loop, ask the coordinator for a task,
	// read the task's input from one or more files, execute the task, write
	// the task's output to one or more files, and again ask the coordinator for a new task.

	for {
		task := CallGetTask()
		if task == nil {
			continue
		}
		if task.Quit { // told to stop.
			break
		}
		time.Sleep(time.Second)
		if err := readAndExecuteTask(task, mapf, reducef); err != nil {
			log.Fatal(err)
		}
	}
}

func makeIntermediateKVStore(mapOut []KeyValue, numReduce int) [][]KeyValue {
	estimatedChunkSize := len(mapOut) / numReduce
	intermediate := make([][]KeyValue, numReduce)
	for i := 0; i < numReduce; i++ {
		intermediate[i] = make([]KeyValue, 0, estimatedChunkSize)
	}

	for _, val := range mapOut {
		idx := ihash(val.Key) % numReduce
		intermediate[idx] = append(intermediate[idx], val)
	}

	return intermediate
}

func saveMapOutputForReducer(mapTask *GetTaskReply, intermediate [][]KeyValue) error {
	errChan := make(chan error, mapTask.NumReduce)
	var wg sync.WaitGroup

	for i := 0; i < mapTask.NumReduce; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			taskFileName := fmt.Sprintf("mr-%d-%d.json", mapTask.TaskNumber, i)
			jsonData, err := json.Marshal(intermediate[i])
			if err != nil {
				errChan <- err
				return
			}
			err = os.WriteFile(taskFileName, jsonData, 0644)
			if err != nil {
				errChan <- err
				return
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		return <-errChan
	}
	return nil
}

func executeMapTask(mapTask *GetTaskReply, mapf func(string, string) []KeyValue) error {
	// The map phase should divide the intermediate keys into buckets for numReduce reduce tasks
	// Each mapper should create numReduce intermediate files for consumption by the reduce tasks.
	filename := mapTask.InputFiles[0] // maps only have one input file.
	contents, err := readFile(filename)
	if err != nil {
		return err
	}
	mapOut := mapf(filename, contents)
	intermediate := makeIntermediateKVStore(mapOut, mapTask.NumReduce)
	return saveMapOutputForReducer(mapTask, intermediate)
}

func loadSingleReduceInput(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var kvs []KeyValue
	if err := decoder.Decode(&kvs); err != nil {
		return nil, err
	}

	return kvs, nil
}

func loadAllInputForReduce(reduceTask *GetTaskReply) ([]KeyValue, error) {
	var reducerInput []KeyValue
	var mu sync.Mutex
	var wg sync.WaitGroup

	numFiles := len(reduceTask.InputFiles)
	singleFileInputChan := make(chan []KeyValue, numFiles)
	errChan := make(chan error, numFiles)

	for _, filename := range reduceTask.InputFiles {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			kvs, err := loadSingleReduceInput(filename)
			if err != nil {
				errChan <- err
				return
			}
			singleFileInputChan <- kvs
		}(filename)
	}

	wg.Wait()
	close(singleFileInputChan)
	close(errChan)

	if len(errChan) > 0 {
		return nil, <-errChan
	}

	for kvs := range singleFileInputChan {
		mu.Lock()
		reducerInput = append(reducerInput, kvs...)
		mu.Unlock()
	}

	return reducerInput, nil
}

func reduceAndSaveOutputToFile(reduceTask *GetTaskReply, sortedReducerInput []KeyValue, reducef func(string, []string) string) error {
	// We write to a tmp file and then rename at the end incase our reducer crashes.
	oname := fmt.Sprintf("mr-out-%d-tmp", reduceTask.TaskNumber)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()

	i := 0
	for i < len(sortedReducerInput) {
		j := i + 1
		for j < len(sortedReducerInput) && sortedReducerInput[j].Key == sortedReducerInput[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, sortedReducerInput[k].Value)
		}
		output := reducef(sortedReducerInput[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", sortedReducerInput[i].Key, output)

		i = j
	}

	finalName := fmt.Sprintf("mr-out-%d", reduceTask.TaskNumber)

	return os.Rename(oname, finalName)
}

func executeReduceTask(reduceTask *GetTaskReply, reducef func(string, []string) string) error {
	reducerInput, err := loadAllInputForReduce(reduceTask)
	if err != nil {
		return err
	}

	sort.Sort(ByKey(reducerInput))

	return reduceAndSaveOutputToFile(reduceTask, reducerInput, reducef)
}

func readAndExecuteTask(task *GetTaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {
	var err error
	if task.IsMap {
		// fmt.Printf("running a map %v\n", task)
		err = executeMapTask(task, mapf)
	} else {
		// fmt.Printf("running a reduce %v\n", task)
		err = executeReduceTask(task, reducef)
	}

	return err
}

func CallGetTask() *GetTaskReply {

	// declare an argument structure.
	args := GetTaskArgs{}

	// declare a reply structure.
	reply := GetTaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		reply.Quit = true
		return &reply
	}

	if len(reply.InputFiles) > 0 {
		return &reply
	}

	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
