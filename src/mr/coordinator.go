package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const ASSUME_WORKER_DEAD_AFTER = 10 * time.Second

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
	attemptsWithoutWork int
}

func reclaimWorkIfWorkerDied(expectedFiles []string, notFinishedCallback func()) {
	time.Sleep(ASSUME_WORKER_DEAD_AFTER)

	for _, filename := range expectedFiles {
		if !fileExists(filename) {
			notFinishedCallback()
			return
		}
	}
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
				c.attemptsWithoutWork = 0
				reply.InputFiles = []string{filename}
				reply.NumReduce = c.numReduce
				reply.IsMap = true
				reply.TaskNumber = mrState.taskNumber
				c.processedInputFiles[filename].mapperStarted = true
				reclaimWorkCb := func() {
					c.mu.Lock()
					fmt.Printf("in mapper reclaim work CB for file %s\n", filename)
					c.processedInputFiles[filename].mapperStarted = false
					c.mu.Unlock()
				}
				go reclaimWorkIfWorkerDied(c.mapperOutputFileNames(mrState.taskNumber), reclaimWorkCb)
			}
			return nil
		}
	}

	// All mappers have started. We can now try to start the reducers
	for i := range c.reducerStarted {
		if c.readyToStartReducer(i) {
			c.mu.RUnlock()
			c.mu.Lock() // Acquire the write lock
			defer c.mu.Unlock()

			// double check the condition after acquiring the write lock.
			if !c.reducerStarted[i] {
				c.attemptsWithoutWork = 0
				reply.InputFiles = c.reducerInputFileNames(i)
				reply.IsMap = false
				reply.TaskNumber = i
				c.reducerStarted[i] = true
				reclaimWorkCb := func() {
					c.mu.Lock()
					fmt.Printf("in reducer reclaim work CB for reducerNumber %d\n", i)
					c.reducerStarted[i] = false
					c.mu.Unlock()
				}
				go reclaimWorkIfWorkerDied(c.reducerOutputFileNames(i), reclaimWorkCb)
			}
			return nil
		}
	}

	c.mu.RUnlock()
	c.mu.Lock() // Acquire the write lock
	defer c.mu.Unlock()

	c.attemptsWithoutWork += 1
	if c.attemptsWithoutWork > 3 { // kinda arbitrary
		reply.Quit = true
	}

	return nil
}

func (c *Coordinator) mapperOutputFileNames(mapNumber int) []string {
	filenames := make([]string, c.numReduce)
	for i := 0; i < c.numReduce; i++ {
		// reducers need all mappers to be done! We get all mapper inputs
		// that have the same reduce task number
		filenames[i] = fmt.Sprintf("mr-%d-%d.json", mapNumber, i)
	}

	return filenames
}

func (c *Coordinator) reducerOutputFileNames(reducerNumber int) []string {
	return []string{fmt.Sprintf("mr-out-%d", reducerNumber)}
}

func (c *Coordinator) reducerInputFileNames(reducerNumber int) []string {
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

	for _, reducerInputFilename := range c.reducerInputFileNames(reducerNumber) {
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
