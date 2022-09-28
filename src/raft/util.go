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
