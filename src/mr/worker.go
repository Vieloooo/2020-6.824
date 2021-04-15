package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"path/filepath"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
	"regexp"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		allover:= true
		args := ExampleArgs{}
		//ask if all work done
		fmt.Printf("ask if all work done\n")
		call("Master.IfDone", &args, &allover)
		if allover {
			fmt.Printf("all work done \n")
			return
		}
		fmt.Printf("no,start to fetch a new work\n")
		//not over, ask task

		reply := Task{}
		reply.Target= "nowork"
		//fmt.Println(reply)
		call("Master.AskTask", &args,&reply)
		fmt.Println(reply)
		//if get a task
		if reply.Target != "nowork" {
			if reply.T == MAP {
				file := reply.Target
				//do map
				DoMap(file, mapf)
			} else {

				//do reudce
				DoReduce(reply.Target,reducef)
			}
		}else{		//no task
			fmt.Printf("no work assigned,waiting...\n")
			time.Sleep(time.Second*5)
		}
	}

}


func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	fmt.Printf("after a rpc call\n")
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
func DoMap(filename string, mapf func(string, string) []KeyValue) {
	fmt.Printf("a new map task processing, filename:\n")
	fmt.Println(filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	//divide kva into mr-filename-(0~9)
	sort.Sort(ByKey(kva))

	//get nreduce
	Nargs:= ExampleArgs{}
	nr:= 10
	call("Master.GetNreduce",&Nargs,&nr)
	//
	//divide kva to temp json file
		//new temp files and json encoders
	tempfiles := make([]*os.File,nr)
	encoders:= make([]*json.Encoder,nr)
	for i:=0; i<nr; i++{
		tempfiles[i],_= ioutil.TempFile("mr-tmp","mr-tmp-*")
		encoders[i]= json.NewEncoder(tempfiles[i])
	}
		//write kva to temp files by json encoders
	for _,kv:= range kva{
		index:= ihash(kv.Key)%nr
		err:= encoders[index].Encode(&kv)
		if err!= nil{
			fmt.Printf("json encode error\n")
			panic("json encode failed")
		}
	}
	fmt.Printf("generate kvs to json temps \n")
	//
	//ask for submit
	submitFile := filename
	reply:= false
	call("Master.AskSubmit",submitFile,&reply)
	if(reply){
		fmt.Printf("submision allowed\n")
		//get file prefix
		byteName:= []byte(filename)
		mid:= byteName[3:len(byteName)-4]
		tempFilePrefix:= "mr-"+string(mid)+"-"
		fmt.Println(tempFilePrefix)
		//delete all files with the same names
		for i,f:= range tempfiles{
			name:= tempFilePrefix+ strconv.Itoa(i)
			oldpath:= filepath.Join(f.Name())
			fmt.Printf("rename %s to %s\n",oldpath,name)
			os.Rename(oldpath,name)
			f.Close()
		}
		call("Master.MapSubmit",submitFile,&reply)
	}else{
		//del all temp files
		for _,f:=range tempfiles{
			f.Close()
		}
	}
}
func DoReduce(number string, reducef func(string, []string) string) {
	// all files are in main/mr-prefix-0~9
	//from mr-*-number => mr-out-number
	//1. read files named in mr-*-$number
	intermediate:=make([]KeyValue,0)
	pattern:= `mr-.*-`+number
	target:=make([]string,0)
	root:= "./"
	dir,err:= ioutil.ReadDir(root)
	if err!=nil{
		panic("open dir error")
	}
	for _,fi := range dir{
		if !fi.IsDir(){
			nm:=fi.Name()
			//fmt.Println(nm)
			matched,_:=regexp.MatchString(pattern,nm)
			if matched{
				//fmt.Println(pattern,nm)
				target = append(target, nm)
			}
		}
	}
	//fmt.Printf("filelist:\n\n")
	//fmt.Println(target)
	//decode json file
	for _,f:=range target{
		fil,er:=os.Open(f)
		if er!=nil{
			panic("file open failed in reduce work")
		}
		dec:=json.NewDecoder(fil)
		for {
			var kv KeyValue
			if erro := dec.Decode(&kv); erro != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		fil.Close()
	}
	//all kvs are transfered from json files to intermediate
	sort.Sort(ByKey(intermediate))
	ofile,err:= ioutil.TempFile("mr-tmp","mr-*")
	if err!=nil{
		panic("create temp files failed in reduce work")
	}
	//copy from mrseq
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	// ask for submit

	//allowed, change name
	outputName:="mr-out-"+number
	os.Rename(filepath.Join(ofile.Name()),outputName)
	ofile.Close()
	// submit

}
