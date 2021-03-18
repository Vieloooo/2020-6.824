package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Master struct {
	// Your definitions here.
}

// state structure
type taskState map[string]int

var tState taskState

const (
	notAssigned = iota
	processing
	timeout
	Done
)

type workerState int

const (
	availiable = iota
	working
	down
	tooSlow
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) server() {
	rpc.Register(m)
	//register rpcs
	var Q query = 1
	rpc.Register(Q)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := masterSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	// instial task (files) state
	initFileState(files)
	//
	m.server()
	return &m
}

func initFileState(files []string) {
	for _, ff := range files {
		tState[ff] = notAssigned
	}
}
