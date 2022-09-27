package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func commandToString(command interface{}) string {
	str := fmt.Sprintf("%v", command)

	if len(str) > 6 {
		return str[0:3] + "..."
	}
	return fmt.Sprintf("%6s", str)
}

func (rf *Raft) accept(commitIndex int32, term int) bool {
	//return rf.term < term || rf.commitIndex <= commitIndex
	if rf.commitIndex < commitIndex {
		return true
	} else if rf.commitIndex > commitIndex {
		return false
	} else {
		return commitIndex == -1 || rf.logs[int(commitIndex)].term <= term
	}
}
