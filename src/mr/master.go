package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
}

// state structure
type taskState struct {
	state    map[string]int
	costTime time.Duration
	lk       sync.Mutex
}

var tState = new(taskState)

const (
	notAssigned = iota
	processing
	Done
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

type Query int

func (m *Master) server() {
	rpc.Register(m)
	//register rpcs
	Q := new(Query)
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
	//launch timeout checker
	//if task is working counting time

	m.server()
	return &m
}

func initFileState(files []string) {
	tState.lk.Lock()
	defer tState.lk.Unlock()
	for _, ff := range files {
		tState.state[ff] = notAssigned
	}
}

func (Q *Query) assignMapper(args *argsAskTask, reply *replyAskTask) error {
	args.file= "hahaha"
	tState.lk.Lock()
	defer tState.lk.Unlock()
	for file, _ := range tState.state {
		reply.tasklist = append(reply.tasklist,file)
		tState.state[file] = processing
	}
	t := time.NewTimer(time.Second*10)
	//reassign the timeout work
	go  func(){
		<-t.C
		tState.lk.Lock()
		defer tState.lk.Unlock()
		for f,s := range tState.state{
			if s == processing{
				ReAssign(f)
			}
		}
	}()
	return nil
}

func ReAssign(filename string){
	args:=
	call("Wrpc.HandleReassign",   ,)
}

