package raft

import (
	"fmt"
)

func commandToString(command interface{}) string {
	str := fmt.Sprintf("%v", command)

	if len(str) > 6 {
		return str[0:3] + "..."
	}
	return fmt.Sprintf("%6s", str)
}

func (rf *Raft) acceptVote(args *RequestVoteArgs) bool {
	length := len(rf.logs)

	if length == 0 {
		return true
	}

	lastLog := rf.logs[length-1]

	if lastLog.Term < args.LastLogTerm {
		return true
	} else if lastLog.Term > args.LastLogTerm {
		return false
	} else {
		return args.LogsLength >= length
	}

}

func (rf *Raft) addLogEntry(entry *LogEntry) int {
	rf.appendLogLock.Lock()
	defer rf.appendLogLock.Unlock()

	index := len(rf.logs)
	entry.Index = index
	rf.appendLog(entry)

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendLogEntryToBuffer(i, entry)
		}
	}

	return index
}

func (rf *Raft) flushLog(commitIndex int) {
	for i := rf.applyIndex + 1; i <= commitIndex; i++ {
		item := rf.logs[i]
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: item.Command, CommandIndex: item.Index + 1}
		//logger.Debugf("raft[%d]向applyCh输入数据 CommandIndex=%d", rf.me, item.Index+1)
		rf.applyIndex++
	}
}

//func (rf *Raft) binarySearch(start, end int) int {
//
//	n := len(rf.peers)
//	mid := (start + end) >> 1
//	med := mid + 1
//
//	return 1
//}
//
//func (rf *Raft) test(n, index int) bool {
//	count := 1
//
//	for _, d := range rf.peerInfos {
//		if d.index >= index {
//			count++
//			if count >= n {
//				return true
//			}
//		}
//	}
//
//	return false
//}
