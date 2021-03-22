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
	"sort"
	"strconv"
)

//
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


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
//reducer input
type Kvset map[string][]string
var Rinput []Kvset
//handle reassign
func (wr *Wrpc)HandleReassign(ARgs *FileName,Reply *Confirm)error{
	go Amapper(*MF,ARgs.name)
	Reply.yes = true
	return nil
}
 var gbkv  = GlobalKV{}
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
	args:= Confirm{}
	args.yes = true
	reply := Tasklist{}
	call("Query.AssignMapper",&args,&reply)
	for _, task := range reply.list {
		go Amapper(mapf, task)
	}
	//go run multi mapper

	//update
}



//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, ARgs interface{}, Reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
	IfmergeARgs := FileName{}
	IfmergeARgs.name = filename
	IfmergeReply := Confirm{}
	IfmergeReply.yes = false
	call("Query.IfMerge", &IfmergeARgs, &IfmergeReply)
	//if task have not done, merge it to gbkv
	if IfmergeReply.yes {
		gbkv.mu.Lock()
		gbkv.gkv = append(gbkv.gkv, middlekv...)
		gbkv.mu.Unlock()
		call("Query.TaskDone",&IfmergeARgs,&IfmergeReply)
	}
	return
}
 // main func for reducer
 func Areducer (index int){
	for k,v := range Rinput[index]{
		output := (*RF)(k, v)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile[index], "%v %v\n", k, output)

	}
 }
func (wr *Wrpc)Server(){
	err:= rpc.Register(wr)
	if err != nil{
		panic("worker rpc down ")
	}
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
 var ofile  [] *os.File
func (wr *Wrpc)RunReducer(ARgs *NReducer,Reply *Confirm)error {
	//divide gbkv
	nreduce := ARgs.nr
	Reply.yes = true
	gbkv.mu.Lock()
	defer gbkv.mu.Unlock()
	sort.Sort(ByKey(gbkv.gkv))
	//init

	Rinput = make([]Kvset,nreduce)
	for i:=0 ; i<nreduce ; i++{
		Rinput[i]= make(Kvset)
	}
	// run nr reducer
	// assign reducer to work
	//new nr output files
	onamePrefix := "mr-out-"
	ofile = [] *os.File{}
	for i:=0;i<nreduce;i++{
		f, _ := os.Create(onamePrefix + strconv.Itoa(i))
		ofile = append(ofile,f)
	}
	//process raw kv
	i := 0
	for i < len(gbkv.gkv) {
		j := i + 1
		for j < len(gbkv.gkv) && gbkv.gkv[j].Key == gbkv.gkv[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, gbkv.gkv[k].Value)
		}
		//use ihash to distribute kvs
		index:= ihash(gbkv.gkv[i].Key)
		Rinput[index][gbkv.gkv[i].Key] = values
		i = j
	}
	// run nreduce mapper
	for i:=0; i<nreduce; i++{
		go Areducer(i)
	}
	return nil
}