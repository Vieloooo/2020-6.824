package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type GlobalKV struct {
	mu  sync.Locker
	gkv []KeyValue
}
//mapper f reduce f
var MF *func(string, string) []KeyValue
var RF *func(string, []string) string
// worker side rpc variable

type Wrpc struct{

}

//handle reassign
func (wr *Wrpc)HandleReassign(args *argsAskTask,ok *replyIfMerge){
	go Amapper((*MF),args.file)
	ok.confirm = true

}
var gbkv GlobalKV = GlobalKV{}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// save mapf reducef to global
	MF = &mapf
	RF = &reducef
	// launch a rpc server to recieve master msg
	wr := Wrpc{}
	go wr.Server()
	//ask master for task
	argsMapper := argsAskTask{}
	replymapper := replyAskTask{}
	call("Query.assignMapper", &argsMapper, &replymapper)
	for _, task := range replymapper.tasklist {
		go Amapper(mapf, task)
	}
	//go run multi mapper

	//update
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func Amapper(mapf func(string, string) []KeyValue, filename string)  {
	// init a middle kv set
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// append all emit to local kv set
	middlekv := mapf(filename, string(content))
	// after finishing map, ask master if it is allowed to merge to global kv
	IfmergeArgs := argsIfMerge{}
	IfmergeArgs.filename = filename
	IfmergeReply := replyIfMerge{}
	IfmergeReply.confirm = false
	call("Query.IfMerge", &IfmergeArgs, &IfmergeReply)
	if IfmergeReply.confirm {
		gbkv.mu.Lock()
		gbkv.gkv = append(gbkv.gkv, middlekv...)
		gbkv.mu.Unlock()

	}
	//return
}
func (wr *Wrpc)Server(){
	rpc.Register(wr)
	//register rpcs
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1235")
	//sockname := masterSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l,nil)
}