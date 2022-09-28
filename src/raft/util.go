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

	if lastLog.term < args.LastLogTerm {
		return true
	} else if lastLog.term > args.LastLogTerm {
		return false
	} else {
		return args.LogsLength >= length
	}

}

func (rf *Raft) addLogEntry(entry *LogEntry) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.logs)
	entry.index = index
	rf.logs = append(rf.logs, entry)

	for i := range rf.peers {
		if i != rf.me {
			rf.sendLogEntryToBuffer(i, entry)
		}
	}

	return index
}

func (rf *Raft) flushLog(commitIndex int) {
	for i := rf.applyIndex + 1; i <= commitIndex; i++ {
		item := rf.logs[i]
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: item.command, CommandIndex: item.index + 1}
		logger.Debugf("raft[%d]向applyCh输入数据 CommandIndex=%d", rf.me, item.index+1)
		rf.applyIndex++
	}
}
