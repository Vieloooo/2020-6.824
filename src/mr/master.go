package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Master struct {
	// Your definitions here.
	files     []string
	Nreduce   int
	fileState map[string]bool
	taskQueue []task
	MapDone bool
	over      bool
}

// Your code here -- RPC handlers for the worker to call.

//task state
type task struct {
	T          int
	processing bool
	st         time.Time
	target     string
	td 			bool//task done
}

const (
	MAP = iota
	REDUCE
)

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//

func (m *Master) server() {
	_ = rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	_ = os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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


	return m.over
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.fileState= make(map[string]bool)
	// Your code here.
	//init master
	m.files = files
	m.Nreduce = nReduce
	m.over = false
	m.MapDone = false
	for _, f := range files {
		m.fileState[f] = false
		m.taskQueue = make([]task, 0)
	}
	//add map tasks to queue
	for _, f := range files {
		atask := task{
			T:          MAP,
			processing: false,
			target:     f,
			st: time.Now(),
		}
		m.taskQueue = append(m.taskQueue, atask)
	}
	fmt.Println(m.taskQueue)
	m.server()
	return &m
}

//first first unassigned task return
func (m *Master) AskTask(args *ExampleArgs, reply *task) error {
	for _, t := range m.taskQueue {
		if !t.processing {
			reply.T = t.T
			reply.target = t.target
			t.processing = true
			t.st = time.Now()
			return nil
		}

	}
	reply.target = "Please wait"
	return nil
}

//check done
func (m *Master) IfDone(args *ExampleArgs, reply *DoneReply) error {
	reply.yes = m.over
	return nil
}
//submit a map temp file
func (m *Master)AskSubmit(args *SubmitArgs,reply *DoneReply)error{
	if(m.fileState[args.file]){
		reply.yes= false
		return nil
	}else{
		m.fileState[args.file]=true
		reply.yes= true
		m.MapChecker()
		return nil
	}
}
func (m *Master)MapChecker(){
	for _,y:= range m.fileState{
		if !y{
			return
		}
	}
	m.MapDone= true

	m.GenReduceTask()
}
func (m *Master)GenReduceTask(){
	for i:=0;i<m.Nreduce;i++{
		atask:=task{}
		atask.processing= false
		atask.T= REDUCE
		atask.td = false
		atask.target = strconv.Itoa(i)
		m.taskQueue= append(m.taskQueue,atask)
	}
}