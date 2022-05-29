package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		job := getJob()
		if job.JobTypeY == UNFINISH {
			time.Sleep(time.Second)
		} else if job.JobTypeY == DOMAP {
			doMapJob(job.FileName, job.NReduce, job.JobId, mapf)
			reportDone(job.JobId)
		} else if job.JobTypeY == DOREDUCE {
			doReduce(job.JobId, job.NMap, reducef)
			reportDone(job.JobId)
		} else {
			return
		}
	}
}

func getJob() Job {
	args := ExampleReply{}

	reply := Job{}

	ok := call("Coordinator.DistributeJob", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func doMapJob(filename string, nReduce int, jobid int, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
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
	intermediate = append(intermediate, kva...)

	kvs := make([][]KeyValue, nReduce)

	for _, v := range intermediate {
		idx := ihash(v.Key) % nReduce
		kvs[idx] = append(kvs[idx], v)
	}

	for idx, kv := range kvs {
		ofilename := fmt.Sprintf("mr-tmp-%v-%v", jobid, idx)
		ofile, _ := os.Create(ofilename)
		enc := json.NewEncoder(ofile)
		for _, v := range kv {
			enc.Encode(&v)
		}
		if err := ofile.Close(); err != nil {
			log.Fatalf("close file error %v", ofilename)
		}
	}
}

func readFromFile(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}

func doReduce(jobid int, nMap int, reducef func(key string, values []string) string) {

	ifilenames := make([]string, nMap)

	for mapTask := 0; mapTask < nMap; mapTask++ {
		ifilenames[mapTask] = fmt.Sprintf("mr-tmp-%v-%v", mapTask, jobid)
	}

	intermediate := readFromFile(ifilenames)
	sort.Sort(ByKey(intermediate))

	ofilename := fmt.Sprintf("mr-tmp-%v", jobid)
	ofile, _ := os.Create(ofilename)

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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()
	oname := fmt.Sprintf("mr-out-%d", jobid)
	os.Rename(ofile.Name(), oname)
}

func CallExample() {

	// declare an argument structure.
	args := ExampleReply{}

	// fill in the argument(s).
	args.Y = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func reportDone(jobid int) {
	args := JobStats{}
	args.JobId = jobid
	args.Status = JOBDONE
	reply := ExampleReply{}
	ok := call("Coordinator.JobDone", &args, &reply)
	if ok {
		fmt.Printf("report map %v is done\n", jobid)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
