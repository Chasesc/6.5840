package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	taskNumber          int
	mu                  sync.RWMutex
	numReduce           int
	processedInputFiles map[string]bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.RLock()

	for filename, processed := range c.processedInputFiles {
		if !processed {
			c.mu.RUnlock()
			c.mu.Lock() // Acquire the write lock
			defer c.mu.Unlock()

			c.taskNumber += 1

			// double check the condition after acquiring the write lock.
			if !c.processedInputFiles[filename] {
				reply.InputFiles = []string{filename}
				reply.NumReduce = c.numReduce
				reply.IsMap = true
				reply.TaskNumber = c.taskNumber
				c.processedInputFiles[filename] = true
			}
			return nil
		}
	}

	c.mu.RUnlock()
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Your code here.
	for _, processed := range c.processedInputFiles {
		if !processed {
			return false
		}
	}

	return false // FIXME: return true when the reducers finish.
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	processedInputFiles := make(map[string]bool, len(files))

	for _, filename := range files {
		processedInputFiles[filename] = false
	}

	c := Coordinator{processedInputFiles: processedInputFiles, numReduce: nReduce}

	c.server()
	return &c
}
