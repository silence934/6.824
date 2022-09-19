package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

//心跳参数
type HeartbeatArgs struct {
	WorkStatus int32
	Address    string
	ServerName string
}

//请求任务的参数
type TaskArgs struct {
	ServerName string
}

//reduce任务请求map任务结果节点的请求参数
type ReduceTaskArgs struct {
	MpaTaskId   int
	ReduceIndex int
}

//已经完成map task的 work信息
type MapWorkInfo struct {
	Id         int
	Address    string
	ServerName string
}

//请求任务的返回
type TaskInfo struct {
	Id int
	//0.没有任务 1. map任务  2. reduce任务
	TaskType int
	//map任务
	Files   []string
	NReduce int
	//reduce任务
	Index int
	Works []MapWorkInfo
}

//完成任务的参数
type CompleteTaskArgs struct {
	//0.没有任务 1. map任务  2. reduce任务
	TaskType  int
	Id        int
	IsSuccess bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
