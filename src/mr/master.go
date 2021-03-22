package mr

import (
	"github.com/pkg/errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
	"fmt"
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
var allTaskDone bool = false

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
// the RPC argument and Reply types are defined in rpc.go.
//

type Query int
//Query func
func (Q *Query)IfMerge(ARgs *FileName,Reply *Confirm)error{
	file := ARgs.name
	tState.lk.Lock()
	defer tState.lk.Unlock()
	if tState.state[file]!=Done{
		Reply.yes = true
	}

	return nil
}
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
var Nr int
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	// initial task (files) state
	initFileState(files)

	Nr = nReduce
	m.server()
	return &m
}

func initFileState(files []string) {
	tState.lk.Lock()
	tState.state =	make(map[string]int)
	for _, ff := range files {
		tState.state[ff] = notAssigned
	}
	tState.lk.Unlock()

}

func (Q *Query) AssignMapper(Args *Confirm, Reply *Tasklist) error {
	if !Args.yes{
		return errors.New("assignerror ")
	}
	tState.lk.Lock()
	for file := range tState.state {
		Reply.list = append(Reply.list,file)
		tState.state[file] = processing
	}
	tState.lk.Unlock()
	t := time.NewTicker(time.Second*10)
	//reassign the timeout work
	go  func(){
		<-t.C
		tState.lk.Lock()
		defer tState.lk.Unlock()
		for f,s := range tState.state{
			if s != Done{
				ReAssign(f)
			}
		}
		return
	}()
	return nil
}
// flag a down task
func (Q *Query)TaskDone(ARgs *FileName,Reply *Confirm)error {
	tState.lk.Lock()
	defer tState.lk.Unlock()
	tState.state[ARgs.name]=Done
	//check if all work down
	for _,f:= range tState.state{
		if f != Done{
			return nil
		}
	}
	allTaskDone = true
	ar:= NReducer{}
	ar.nr = Nr
	re:= Confirm{}
	re.yes = false
	callWorker("Wrpc.RunReducer",&ar,&re)
	return nil
}
func ReAssign(filename string){
	ARgs := FileName{}
	ARgs.name = filename
	Reply := Confirm{}
	Reply.yes = false
	callWorker("Wrpc.HandleReassign",&ARgs,&Reply)
}

func callWorker(rpcname string, ARgs interface{}, Reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1235")
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, ARgs, Reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

