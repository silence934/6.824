package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type worksHeartbeat struct {
	WorkStatus int32
	Address    string
	ServerName string
	Time       time.Time
}

type MapTask struct {
	id             int
	files          []string
	execServerName string
	startTime      time.Time
}

type ReduceTask struct {
	index          int
	execServerName string
	startTime      time.Time
}

type Coordinator struct {
	// Your definitions here.
}

var (
	_nReduce       int
	_mapTaskNumber int
	//	_completedRudeTask []MapTask
	_completedRudeTask = 0
	_runningReduceTask = map[int]ReduceTask{}
	_unStartReduceTask []ReduceTask

	_completedTask   []MapTask
	_runningTask     = map[int]MapTask{}
	_unStartTask     []MapTask
	_mutex1          sync.Mutex
	_worksHeartbeat  = map[string]worksHeartbeat{}
	_doneCoordinator = make(chan struct{})
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FindTask(args *TaskArgs, reply *TaskInfo) error {

	_mutex1.Lock()
	defer _mutex1.Unlock()
	if len(_unStartTask) > 0 {
		allotMapTask(args, reply)
	} else if len(_unStartReduceTask) > 0 {
		allotReduceTask(args, reply)
	}
	return nil
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, shutdown *bool) error {
	_mutex1.Lock()
	defer _mutex1.Unlock()
	*(shutdown) = false
	_worksHeartbeat[args.ServerName] = worksHeartbeat{args.WorkStatus, args.Address, args.ServerName, time.Now()}
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, shutdown *bool) error {
	_mutex1.Lock()
	defer _mutex1.Unlock()
	if args.TaskType == 1 {
		*shutdown = false

		mapTask := _runningTask[args.Id]
		_completedTask = append(_completedTask, mapTask)
		delete(_runningTask, args.Id)

		if len(_completedTask) == _mapTaskNumber {
			for i := 0; i < _nReduce; i++ {
				_unStartReduceTask = append(_unStartReduceTask, ReduceTask{index: i})
			}
		}

	} else if args.TaskType == 2 {
		*shutdown = false
		delete(_runningReduceTask, args.Id)
		_completedRudeTask++
		if _completedRudeTask == _nReduce {
			_doneCoordinator <- struct{}{}
			close(_doneCoordinator)
			*shutdown = true
		}
	}
	return nil
}

func allotMapTask(args *TaskArgs, reply *TaskInfo) {
	//todo 并发
	task := _unStartTask[len(_unStartTask)-1]
	_unStartTask = _unStartTask[:len(_unStartTask)-1]
	task.startTime = time.Now()
	task.execServerName = args.ServerName
	_runningTask[task.id] = task

	reply.Id = task.id
	reply.TaskType = 1
	reply.NReduce = _nReduce
	//reply.Files = []string{task.filename}
	reply.Files = task.files
}

func allotReduceTask(args *TaskArgs, reply *TaskInfo) {
	//todo 并发
	task := _unStartReduceTask[len(_unStartReduceTask)-1]
	_unStartReduceTask = _unStartReduceTask[:len(_unStartReduceTask)-1]
	task.startTime = time.Now()
	task.execServerName = args.ServerName
	_runningReduceTask[task.index] = task

	var works []MapWorkInfo
	for _, task := range _completedTask {
		//todo 如果address没有
		work := MapWorkInfo{task.id, _worksHeartbeat[task.execServerName].Address, task.execServerName}
		works = append(works, work)
	}

	reply.Id = task.index
	reply.TaskType = 2
	reply.Index = task.index
	reply.Works = works
}

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

func (c *Coordinator) Done() bool {
	<-_doneCoordinator
	for _, work := range _worksHeartbeat {
		if work.Address != "" {
			client, err := rpc.DialHTTP("tcp", work.Address)
			if err != nil {
				fmt.Printf("关闭work[%v]节点失败:%v\n", work, err.Error())
			}

			req := 1
			resp := 1
			err = client.Call("WorkServer.Shutdown", &req, &resp)
		}
	}
	return true
}

func check() {
	for true {
		if isChanClose(_doneCoordinator) {
			break
		}
		_mutex1.Lock()

		fmt.Printf("------------\nmap task:%d %d %d\n", len(_unStartTask), len(_runningTask), len(_completedTask))
		fmt.Printf("reduce map task:%d %d %d\n", len(_unStartReduceTask), len(_runningReduceTask), _completedRudeTask)
		fmt.Printf("pid:%d\n", os.Getpid())
		fmt.Printf("------------\n")

		for key, task := range _runningTask {
			if task.startTime.Add(10 * time.Second).Before(time.Now()) {
				_unStartTask = append(_unStartTask, task)
				delete(_runningTask, key)
			}
		}

		for i, task := range _completedTask {
			if _worksHeartbeat[task.execServerName].Time.Add(10 * time.Second).Before(time.Now()) {
				_unStartTask = append(_unStartTask, task)
				_completedTask = append(_completedTask[:i], _completedTask[i+1:]...)
			}
		}

		for key, task := range _runningReduceTask {
			if task.startTime.Add(10 * time.Second).Before(time.Now()) {
				_unStartReduceTask = append(_unStartReduceTask, task)
				delete(_runningReduceTask, key)
			}
		}

		_mutex1.Unlock()
		time.Sleep(5 * time.Second)
	}
}

func isChanClose(ch chan struct{}) bool {
	select {
	case _, received := <-ch:
		return !received
	default:
	}
	return false
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	_nReduce = nReduce
	for i, file := range files {
		//_unStartTask = append(_unStartTask, MapTask{filename: file})
		_unStartTask = append(_unStartTask, MapTask{id: i, files: []string{file}})
	}
	_mapTaskNumber = len(_unStartTask)

	// Your code here.

	c.server()
	go check()
	return &c
}
