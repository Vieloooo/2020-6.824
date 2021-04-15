package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu sync.Mutex
	files     []string
	Nreduce   int
	fileState map[string]bool
	taskQueue []Task
	MapDone bool
	over      bool
}

// Your code here -- RPC handlers for the worker to call.

//task state
type Task struct {
	T          int
	Processing bool
	St         time.Time
	Target     string
	TaskDone 			bool//task done
}

const (
	MAP = iota
	REDUCE
)


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
	fmt.Printf("master server lunched\n")
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	fmt.Printf("master init\n")
	//fmt.Println(m)
	for _, f := range files {
		m.fileState[f] = false
		m.taskQueue = make([]Task, 0)
	}
	//add map tasks to queue
	for _, f := range files {
		atask := Task{
			T:          MAP,
			Processing: false,
			Target:     f,
			St: time.Now(),
			TaskDone: false,
		}
		m.taskQueue = append(m.taskQueue, atask)
	}
	fmt.Printf("map task added\n")
	//fmt.Println(m.taskQueue)
	m.server()
	return &m
}

//first first unassigned task return
func (m *Master) AskTask(args *ExampleArgs, reply *Task) error {
	fmt.Printf("master handle ask task\n")
	m.mu.Lock()
	//fmt.Printf("lock mutex \n")
	defer m.mu.Unlock()
	//fmt.Println(m.taskQueue)
	for i, t := range m.taskQueue {
		if !t.Processing {
			reply.T = t.T
			reply.Target = t.Target
			fmt.Println(reply)
			m.taskQueue[i]=Task{
				Processing: true,
				St: time.Now(),
				TaskDone: false,
				T: MAP,
				Target: t.Target,
			}

			return nil
		}

	}


	return nil
}

//check done
func (m *Master) IfDone(args *ExampleArgs, reply *bool) error {
	*reply = m.over

	return nil
}

//submit a map temp file
func (m *Master)AskSubmit(args string,reply *bool)error{
	m.mu.Lock()
	defer m.mu.Unlock()
	*reply = !m.fileState[args]
	return nil
}

// when a map task finished
func(m *Master)MapSubmit(args string,reply *bool)error{
	m.mu.Lock()
	*reply= true
	m.fileState[args]=true
	//m.taskQueue[args].TaskDone= true
	fmt.Printf("%s task submitted\n",args)
	defer m.mu.Unlock()
	for _,y:= range m.fileState{
		if !y{
			fmt.Printf("no all map done\n")
			return nil
		}
	}
	m.MapDone= true
	//if done add reduce task
	for i:=0;i<m.Nreduce;i++{
		atask:=Task{}
		atask.Processing= false
		atask.T= REDUCE
		atask.TaskDone = false
		atask.Target = strconv.Itoa(i)
		m.taskQueue= append(m.taskQueue,atask)
	}
	return nil
}

//get n reudce
func (m *Master)GetNreduce(args *ExampleArgs,reply *int)error{
	*reply= m.Nreduce
	return  nil
}
