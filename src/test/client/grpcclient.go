package main

import (
	"fmt"
	"net/rpc"
)

func main() {
	type AddParma struct {
		Args1 float32
		Args2 float32
	}
	//创建连接
	client, err := rpc.DialHTTP("tcp", "localhost:8081")
	if err != nil {
		panic(err.Error())
	}
	//请求值
	var req float32
	req = 3
	//返回值
	var resp *float32
	//MathUtil.CalculateCircleArea这个是要调用的结构体的方法
	//err = client.Call("MathUtil.CalculateCircleArea", req, &resp)
	parma := AddParma{
		Args1: 12,
		Args2: 3,
	}
	//使用别名进行调用方法
	err = client.Call("golang.CalculateCircleArea", req, &resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("圆的面积为", *resp)
	err = client.Call("golang.AddCircleArea", parma, &resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("相加的结果为:", *resp)

}
