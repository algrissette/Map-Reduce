package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	NumReduce      int             // Number of reduce tasks
	Files          []string        // Files for map tasks, len(Files) is number of Map tasks
	MapTasks       chan Task       // Channel for map tasks
	CompletedTasks map[string]bool // Map to check if task is completed
	Lock           sync.Mutex      // Lock for controlling shared variables
	Tasktype       string          // Indicates Task's Type ("Map" or "Reduce")
	AllTaskDone    bool            // true if all tasks completed, false otherwise
	MapCounter     int             // Counter for map tasks
	ReduceCounter  int             // Counter for reduce tasks
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:      nReduce,
		Files:          files,
		MapTasks:       make(chan Task, len(files)+nReduce),
		CompletedTasks: make(map[string]bool),
		Tasktype:       "Map",
		AllTaskDone:    false,
		MapCounter:     0, // Initialize map counter
		ReduceCounter:  0, // Initialize reduce counter
	}

	// Initialize map tasks
	for i, file := range c.Files {
		mapTask := Task{
			Filename:  file,
			NumMap:    i + 1,
			TaskType:  "Map",
			NumReduce: c.NumReduce,
		}

		c.MapTasks <- mapTask
		c.CompletedTasks[mapTask.Filename] = false
		c.MapCounter++
	}

	c.server()
	return &c
}

// RPC handler for workers to request tasks
func (c *Coordinator) RequestTask(args *EmptyArs, reply *Task) error {

	task, ok := <-c.MapTasks
	if !ok {
		return nil // No task available
	}

	*reply = task
	go c.WaitForWorker(task)

	return nil
}

func (c *Coordinator) WaitForWorker(task Task) {
	time.Sleep(time.Second * 10)

	c.Lock.Lock()
	defer c.Lock.Unlock()

	if !c.CompletedTasks[task.Filename] {
		c.MapTasks <- task
	}
}

// RPC handler for workers to report task completion
func (c *Coordinator) TaskCompleted(args *Task, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if c.Tasktype == "Map" {
		c.CompletedTasks[args.Filename] = true

		// Check if all map tasks are completed
		allMapTasksCompleted := true
		for _, completed := range c.CompletedTasks {
			if !completed {
				allMapTasksCompleted = false
				break
			}
		}

		if allMapTasksCompleted {
			c.StartReduce()
		}
	} else if c.Tasktype == "Reduce" {
		c.CompletedTasks["reducetask"+strconv.Itoa(args.Bucketnum)] = true

		// Check if all reduce tasks are completed
		allReduceTasksCompleted := true
		for i := 0; i < c.NumReduce; i++ {
			if !c.CompletedTasks["reducetask"+strconv.Itoa(i)] {
				allReduceTasksCompleted = false
				break
			}
		}

		if allReduceTasksCompleted {
			c.AllTaskDone = true
		}
	}
	return nil
}

func (c *Coordinator) StartReduce() {

	c.Tasktype = "Reduce" // Switch to reduce phase

	// Start reduce tasks
	for i := 0; i < c.NumReduce; i++ {
		reduceTask := Task{
			Filename:  "reducetask_" + strconv.Itoa(i),
			TaskType:  "Reduce",
			NumReduce: c.NumReduce,
			NumMap:    c.MapCounter,
			Bucketnum: i,
		}

		c.MapTasks <- reduceTask
		c.CompletedTasks["reducetask"+strconv.Itoa(i)] = false
	}
}

// RPC handler for the example RPC
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	return c.AllTaskDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
