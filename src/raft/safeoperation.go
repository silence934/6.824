package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) getPeerIndex(server int) int {
	rf.peerInfos[server].updateIndexLock.Lock()
	defer rf.peerInfos[server].updateIndexLock.Unlock()
	return rf.peerInfos[server].index
}

func (rf *Raft) setPeerIndex(server, index int) {
	rf.peerInfos[server].updateIndexLock.Lock()
	defer rf.peerInfos[server].updateIndexLock.Unlock()
	rf.peerInfos[server].index = index
}

func (rf *Raft) getRole() int32 {
	return atomic.LoadInt32(&rf.role)
}

func (rf *Raft) setRole(old, new int32) bool {
	return atomic.CompareAndSwapInt32(&rf.role, old, new)
}

func (rf *Raft) isLeader() bool {
	return rf.getRole() == leader
}

func (rf *Raft) isFollower() bool {
	return rf.getRole() == follower
}

func (rf *Raft) lastTime() time.Time {
	return rf.lastCallTime
}

func (rf *Raft) appendLog(entry *LogEntry) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.logs)
	entry.index = index
	rf.logs = append(rf.logs, entry)

	return index
}
