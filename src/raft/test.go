package raft

import "fmt"

func main() {
	server := 1
	resp := RequestHeartbeatReply{}
	str := fmt.Sprintf("xxxx -->%d  resp :%v", server, resp)
	fmt.Println(str)
}
