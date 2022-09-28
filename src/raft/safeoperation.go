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

func (rf *Raft) lockSyncLog() bool {
	return atomic.CompareAndSwapInt32(&rf.syncLogLock, 0, 1)
}

func (rf *Raft) unlockSyncLog() {
	rf.syncLogLock = 0
}

func (rf *Raft) lockCheckLog(server int) bool {
	return atomic.CompareAndSwapInt32(&rf.peerInfos[server].checkLogsLock, 0, 1)
}

func (rf *Raft) unlockCheckLog(server int) {
	//logger.Infof("raft[%d] 解锁 %d", rf.me, server)
	rf.peerInfos[server].checkLogsLock = 0
}

func (rf *Raft) syncCountAddGet(index int) int {
	return int(atomic.AddInt32(&rf.logs[index].syncCount, 1))
}
