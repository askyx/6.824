package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

var mu sync.Mutex

const (
	DOMAP = iota
	DOREDUCE
	UNFINISH
	DONE
)

const (
	START = iota
	DOING
	JOBDONE
)

type JobInfo struct {
	job       *Job
	jobstatus int
}

type Coordinator struct {
	JobChannelMap chan *Job
	JobChannelRed chan *Job
	files         []string
	nReduce       int
	NMap          int
	Status        int
	JobMap        []JobInfo
}

func (c *Coordinator) JobDone(args *JobStats, reply *ExampleReply) error {
	c.JobMap[args.JobId].jobstatus = args.Status
	return nil
}

func (c *Coordinator) allJobDone() bool {

	for _, v := range c.JobMap {
		if v.jobstatus != JOBDONE {
			return false
		}
	}
	return true
}

// func (c *Coordinator) handleCrash() {
// 	loop := true
// 	for loop {
// 		time.Sleep(time.Second * 2)
// 		mu.Lock()
// 		if c.allJobDone() {
// 			loop = false
// 		} else {
// 			for _, v := range c.JobMap {
// 				if c.Status == DOMAP {
// 					// if v.jobstatus == DOING {

// 					// }
// 				} else if c.Status == DOREDUCE {

// 				} else {
// 					loop = false
// 				}
// 			}
// 		}
// 		mu.Unlock()
// 	}
// }

func (c *Coordinator) DistributeJob(args *ExampleReply, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()
	if c.Status == DOMAP {
		if len(c.JobChannelMap) > 0 {
			*reply = *<-c.JobChannelMap
			c.JobMap[reply.JobId].jobstatus = DOING
		} else {
			if c.allJobDone() {
				reply.JobTypeY = UNFINISH
				c.Status = DOREDUCE
				c.makeReduceJobs()
			} else {
				reply.JobTypeY = UNFINISH
			}
		}
	} else if c.Status == DOREDUCE {
		if len(c.JobChannelRed) > 0 {
			*reply = *<-c.JobChannelRed
			c.JobMap[reply.JobId].jobstatus = DOING
		} else {
			if c.allJobDone() {
				c.Status = DONE
				reply.JobTypeY = UNFINISH
			} else {
				reply.JobTypeY = UNFINISH
			}
		}
	} else {
		reply.JobTypeY = DONE
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := false

	if c.Status == DONE {
		ret = true
	}
	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.makeMapJobs(files, nReduce)

	// go c.handleCrash()
	c.server()
	return &c
}

func (c *Coordinator) makeMapJobs(files []string, nReduce int) {

	c.JobChannelMap = make(chan *Job, len(files))
	c.JobMap = make([]JobInfo, len(files))
	c.Status = DOMAP
	c.files = files
	c.nReduce = nReduce
	c.NMap = len(files)
	for id, v := range files {
		job := Job{
			JobTypeY: DOMAP,
			FileName: v,
			JobId:    id,
			NReduce:  nReduce,
		}
		c.JobChannelMap <- &job
		c.JobMap[id] = JobInfo{&job, START}
	}
}

func (c *Coordinator) makeReduceJobs() {
	c.JobChannelRed = make(chan *Job, c.nReduce)
	c.Status = DOREDUCE
	c.JobMap = make([]JobInfo, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		job := Job{
			JobTypeY: DOREDUCE,
			JobId:    i,
			NReduce:  c.nReduce,
			NMap:     c.NMap,
		}
		c.JobMap[i] = JobInfo{&job, START}
		c.JobChannelRed <- &job
	}
}
