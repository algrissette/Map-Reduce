package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type EmptyArs struct {
}

type EmptyReply struct {
}

type Task struct {
	Filename  string // Filename = key
	NumMap    int
	NumReduce int
	Bucketnum int    // Number of map tasks
	TaskType  string // Indicates Task's Type ("Map" or "Reduce")
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}