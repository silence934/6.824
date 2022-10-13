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
	length := rf.logLength()

	if length == 0 {
		return true
	}

	lastLog := rf.entry(rf.logLength() - 1)

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

	//fmt.Printf("%d %d\n", len(rf.logs), rf.lastIncludedIndex)
	index := rf.logLength()
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
		item := rf.entry(i)
		rf.applyCh <- ApplyMsg{
			CommandValid:  true,
			Command:       item.Command,
			CommandIndex:  item.Index,
			SnapshotValid: false,
		}
		//fmt.Printf("raft[%d]向applyCh输入数据 %+v\n", rf.me, item)
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

func (rf *Raft) entry(index int) *LogEntry {
	return &rf.logs[rf.logIndex(index)]
}

func (rf *Raft) logIndex(realIndex int) int {
	return realIndex - rf.lastIncludedIndex
}

func (rf *Raft) setLog(log *LogEntry, index int) {
	rf.logs[rf.logIndex(index)] = *log
	rf.persist()
}

func (rf *Raft) appendLog(log *LogEntry) {
	rf.logs = append(rf.logs, *log)
	rf.persist()
}

func (rf *Raft) logLength() int {
	return rf.lastIncludedIndex + len(rf.logs)
}

func (rf *Raft) setTerm(term int32) {
	rf.term = term
	rf.persist()
}
