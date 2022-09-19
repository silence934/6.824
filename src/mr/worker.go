package mr

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

const (
	nonTask              int32 = 0
	mapTaskProcessing    int32 = 1
	mapTaskSuccessful    int32 = 2
	mapTaskFailed        int32 = 3
	reduceTaskProcessing int32 = 4
	reduceTaskSuccessful int32 = 5
	reduceTaskFailed     int32 = 6
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var (
	_mapf                func(string, string) []KeyValue
	_reducef             func(string, []string) string
	_map                 = make(map[int]map[int][]KeyValue)
	_status              int32
	_serverName          string
	_serverStartTime     time.Time
	_lastCallSuccessTime time.Time
	_lastTimeLock        sync.Mutex

	_address    string
	_addressLok sync.Mutex

	_serverStart  = false
	_wg           = sync.WaitGroup{}
	_shutdownFunc context.CancelFunc
)

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	_mapf = mapf
	_reducef = reducef
	_status = nonTask
	_serverName = "work-" + strconv.Itoa(os.Getpid())
	_serverStartTime = time.Now()

	ctx, cancel := context.WithCancel(context.Background())

	_shutdownFunc = cancel

	defer func() {
		fmt.Println("kill this work")
		_wg.Wait()
		fmt.Printf("%s bye\n", _serverName)
	}()

	go heartbeatLoop(ctx)

	for true {
		select {
		case <-ctx.Done():
			return
		default:
			FindTask()
			time.Sleep(time.Second)
		}
	}

}

func heartbeatLoop(ctx context.Context) {
	_wg.Add(1)
	defer _wg.Done()

	args := HeartbeatArgs{_status, "", _serverName}
	shutdown := false

	for true {
		select {
		case <-ctx.Done():
			return
			//fmt.Println("handle", ctx.Err())
		default:
			args.Address = getAddress()
			//fmt.Printf("heartbeatLoop..  %v\n", args)
			call("Coordinator.Heartbeat", &args, &shutdown)
			if shutdown {
				fmt.Printf("Coordinator told me to shut down")
				_shutdownFunc()
			}
			time.Sleep(time.Second)
		}
	}

}

func FindTask() {

	// declare an argument structure.
	args := TaskArgs{_serverName}

	// declare a reply structure.
	reply := TaskInfo{}

	ok := call("Coordinator.FindTask", &args, &reply)
	if ok {
		fmt.Printf("findTask reply task type: %v\n", reply.TaskType)
		completeTaskArgs := CompleteTaskArgs{TaskType: reply.TaskType, Id: reply.Id}
		if reply.TaskType == 1 {
			_status = mapTaskProcessing
			err, res := execMapTask(&reply.Files, reply.NReduce)
			_map[reply.Id] = *res
			if err == nil {
				atomic.CompareAndSwapInt32(&_status, mapTaskProcessing, mapTaskSuccessful)
				completeTaskArgs.IsSuccess = true
			} else {
				atomic.CompareAndSwapInt32(&_status, mapTaskProcessing, mapTaskFailed)
				completeTaskArgs.IsSuccess = false
				fmt.Println(err)
			}
			//&reply.Files
		} else if reply.TaskType == 2 {
			_status = reduceTaskProcessing
			err := execReduceTask(&reply.Works, reply.Index)
			if err == nil {
				atomic.CompareAndSwapInt32(&_status, reduceTaskProcessing, reduceTaskSuccessful)
				completeTaskArgs.IsSuccess = true
			} else {
				atomic.CompareAndSwapInt32(&_status, reduceTaskProcessing, reduceTaskFailed)
				completeTaskArgs.IsSuccess = false
				fmt.Println(err)
			}
		} else {
			//fmt.Println("返回错误任务类型")
			return
		}
		completeTask(completeTaskArgs)

	} else {
		fmt.Printf("call failed!\n")
	}
}

func completeTask(args CompleteTaskArgs) {
	fmt.Printf("complete task:%v\n", args)
	shutdown := false
	call("Coordinator.CompleteTask", &args, &shutdown)
	if shutdown {
		fmt.Println("Coordinator told me to shutdown")
		_shutdownFunc()
	}
}

func execMapTask(files *[]string, nReduce int) (error, *map[int][]KeyValue) {
	fmt.Printf("start map task [%v]\n", *files)

	result := make(map[int][]KeyValue)

	var intermediate []KeyValue
	for _, filename := range *files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := _mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	for _, item := range intermediate {
		i := ihash(item.Key) % nReduce
		if result[i] == nil {
			result[i] = []KeyValue{}
		}
		result[i] = append(result[i], item)
	}
	fmt.Println("map task exec successful!")
	//fmt.Println(_map)
	if !_serverStart {
		go startServer()
		_serverStart = true
	}

	return nil, &result
}

func execReduceTask(works *[]MapWorkInfo, index int) error {
	var intermediate []KeyValue

	fmt.Printf("start reduce task [index:%d]\n", index)
	for _, work := range *(works) {
		intermediate = append(intermediate, callWork(&work, index)...)
	}
	fmt.Println("The result has been fetched from the map task node")
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(index)
	os.Remove(oname)
	ofile, _ := os.Create(oname)

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
		output := _reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return nil
}

func callWork(work *MapWorkInfo, index int) []KeyValue {

	client, err := rpc.DialHTTP("tcp", work.Address)
	if err != nil {
		panic(err.Error())
	}

	req := ReduceTaskArgs{work.Id, index}
	var resp []KeyValue

	err = client.Call("WorkServer.GetMapResult", &req, &resp)
	if err != nil {
		panic(err.Error())
	}
	//容错处理
	return resp
}

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
		setLastTime(time.Now())
		return true
	}

	fmt.Println(err)
	lastSuccessTime := getLastTime()
	if lastSuccessTime.IsZero() {
		if time.Now().Add(-10 * time.Minute).After(_serverStartTime) {
			fmt.Println("Work failed to establish a connection with coordinator ten minutes after it started")
			_shutdownFunc()
		}
	} else if time.Now().Add(-10 * time.Minute).After(lastSuccessTime) {
		fmt.Println("It has been 10 minutes since the last successful access to coordinator")
		_shutdownFunc()
	}

	return false
}

type WorkServer struct {
}

func (w *WorkServer) GetMapResult(req *ReduceTaskArgs, resp *[]KeyValue) error {
	*resp = append(*resp, _map[req.MpaTaskId][req.ReduceIndex]...)
	return nil
}

func (w *WorkServer) Shutdown(a *int, b *int) error {
	fmt.Println("bye,end of the task...!")
	_shutdownFunc()
	return nil
}

func startServer() {
	//初始化结构体
	workServer := WorkServer{}
	// 调用net/rpc的功能进行注册
	//err := rpc.Register(&mathUtil)
	//这里可以使用取别名的方式
	err := rpc.RegisterName("WorkServer", &workServer)
	//判断结果是否正确
	if err != nil {
		fmt.Println(err)
		_status = mapTaskFailed
		return
	}
	//通过HandleHTTP()把mathUtil提供的服务注册到HTTP协议上，方便调用者利用http的方式进行数据传递
	rpc.HandleHTTP()
	//指定端口监听
	listen, err := net.Listen("tcp", "localhost:0")

	setAddress(listen.Addr().String())
	fmt.Println("work server start at:" + _address)

	if err != nil {
		fmt.Println(err)
		_status = mapTaskFailed
		return
	}

	err = http.Serve(listen, nil)
	if err != nil {
		fmt.Println(err)
		_status = mapTaskFailed
	}

}

func setAddress(add string) {
	_addressLok.Lock()
	defer _addressLok.Unlock()
	_address = add
}

func getAddress() string {
	_addressLok.Lock()
	defer _addressLok.Unlock()
	return _address
}

func setLastTime(time time.Time) {
	_lastTimeLock.Lock()
	defer _lastTimeLock.Unlock()
	_lastCallSuccessTime = time
}

func getLastTime() time.Time {
	_lastTimeLock.Lock()
	defer _lastTimeLock.Unlock()
	return _lastCallSuccessTime
}
