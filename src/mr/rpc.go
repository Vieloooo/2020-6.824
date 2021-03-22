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

//
// example to show how to declare the arguments
// and Reply for an RPC.
//


// Add your RPC definitions here.

//ARgs for ask for tasks

type FileName struct {
	name string
}
type Tasklist struct {
	list []string
}
type Confirm struct{
	yes bool
}
type NReducer struct{
	nr int
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
