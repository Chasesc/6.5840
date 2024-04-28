package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

type mapState struct {
	mapperStarted bool
	taskNumber    int
}

type Coordinator struct {
	// Your definitions here.
	mu                  sync.RWMutex
	numReduce           int
	processedInputFiles map[string]*mapState
	reducerStarted      []bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.RLock()

	for filename, mrState := range c.processedInputFiles {
		if !mrState.mapperStarted {
			c.mu.RUnlock()
			c.mu.Lock() // Acquire the write lock
			defer c.mu.Unlock()

			// double check the condition after acquiring the write lock.
			if !c.processedInputFiles[filename].mapperStarted {
				reply.InputFiles = []string{filename}
				reply.NumReduce = c.numReduce
				reply.IsMap = true
				reply.TaskNumber = mrState.taskNumber
				c.processedInputFiles[filename].mapperStarted = true
			}
			return nil
		}
	}

	// All mappers have started. We can now start the reducers
	for i := range c.reducerStarted {
		if c.readyToStartReducer(i) {
			c.mu.RUnlock()
			c.mu.Lock() // Acquire the write lock
			defer c.mu.Unlock()

			// double check the condition after acquiring the write lock.
			if !c.reducerStarted[i] {
				reply.InputFiles = c.reducerFileNames(i)
				reply.IsMap = false
				reply.TaskNumber = i
				c.reducerStarted[i] = true
			}
			return nil
		}
	}

	c.mu.RUnlock()
	return nil
}

func (c *Coordinator) reducerFileNames(reducerNumber int) []string {
	numMappers := len(c.processedInputFiles)
	filenames := make([]string, numMappers)
	for i := 0; i < numMappers; i++ {
		// reducers need all mappers to be done! We get all mapper inputs
		// that have the same reduce task number
		filenames[i] = fmt.Sprintf("mr-%d-%d.json", i, reducerNumber)
	}

	return filenames
}

func (c *Coordinator) readyToStartReducer(reducerNumber int) bool {
	if c.reducerStarted[reducerNumber] {
		return false // already started.
	}

	for _, reducerInputFilename := range c.reducerFileNames(reducerNumber) {
		// our mapper should really create tempfiles and atomically rename so we don't
		// return true when we are still writing to the file, but lets see if I decide to implement that.
		if !fileExists(reducerInputFilename) {
			return false
		}
	}

	return true
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) reducerFinished() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for reducerNumber, thisReducerStarted := range c.reducerStarted {
		if !thisReducerStarted || !fileExists(fmt.Sprintf("mr-out-%d", reducerNumber)) {
			return false
		}
	}

	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.reducerFinished()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	processedInputFiles := make(map[string]*mapState, len(files))
	for i, filename := range files {
		processedInputFiles[filename] = &mapState{taskNumber: i}
	}

	reducerStarted := make([]bool, nReduce)
	c := Coordinator{processedInputFiles: processedInputFiles, numReduce: nReduce, reducerStarted: reducerStarted}

	c.server()
	return &c
}
