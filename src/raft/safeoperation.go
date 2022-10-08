package raft

import (
	lock2 "6.824/lock"
	"strconv"
	"sync/atomic"
	"time"
)

func (rf *Raft) getPeerIndex(server int) int {
	rf.peerInfos[server].updateIndexLock.Lock()
	defer rf.peerInfos[server].updateIndexLock.Unlock()
	index := rf.peerInfos[server].index
	//logger.Infof("raft[%d] getPeerIndex[%d]  --> %d", rf.me, server, index)
	return index
}

func (rf *Raft) updatePeerIndex(server, older, new int) bool {
	rf.peerInfos[server].updateIndexLock.Lock()
	defer rf.peerInfos[server].updateIndexLock.Unlock()
	peer := rf.peerInfos[server]
	if peer.index == older {
		peer.index = new
		//logger.Infof("raft[%d] 修改 peer[%d].index  %d->%d  %d", rf.me, server, older, new, rf.peerInfos[server].index)
		return true
	}
	return false
}

func (rf *Raft) setPeerIndex(server, index int) {
	rf.peerInfos[server].updateIndexLock.Lock()
	defer rf.peerInfos[server].updateIndexLock.Unlock()
	//older := rf.peerInfos[server].index
	rf.peerInfos[server].index = index
	//logger.Infof("raft[%d] 修改 peer[%d].index  %d->%d  %d", rf.me, server, older, index, rf.peerInfos[server].index)
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

func (rf *Raft) lockCheckLog(server int, timeout time.Duration) *lock2.Lock {
	return lock2.Make(&rf.peerInfos[server].checkLogsLock, timeout, strconv.Itoa(rf.me)+" "+strconv.Itoa(server))
	//lock := atomic.CompareAndSwapInt32(&rf.peerInfos[server].checkLogsLock, 0, 1)
	//var timer *time.Timer
	//if lock {
	//	wg := sync.WaitGroup{}
	//	wg.Add(1)
	//	go func(s int, out time.Duration) {
	//		timer = time.NewTimer(out)
	//		wg.Done()
	//		<-timer.C
	//		if !timer.Stop() {
	//			//logger.Errorf("超时")
	//			rf.unlockCheckLog(s)
	//		}
	//	}(server, timeout)
	//	wg.Wait()
	//}
	//logger.Infof("raft[%d] 上锁 %d  %v  %d", rf.me, server, lock, atomic.LoadInt32(&rf.peerInfos[server].checkLogsLock))
	//return lock, timer
}

func (rf *Raft) unlockCheckLog(server int) {
	//logger.Infof("raft[%d] 解锁 %d", rf.me, server)
	rf.peerInfos[server].checkLogsLock = 0
}

func (rf *Raft) syncCountAddGet(index int) int {
	return int(atomic.AddInt32(&rf.logs[index].SyncCount, 1))
}

func (rf *Raft) initPeerInfos() bool {
	if atomic.CompareAndSwapInt32(&rf.initPeers, 0, 1) {
		defer func() { rf.initPeers = 0 }()
		length := len(rf.logs)
		if len(rf.peerInfos) == 0 {
			rf.peerInfos = make([]*peerInfo, len(rf.peers))
			for i := 0; i < len(rf.peerInfos); i++ {
				rf.peerInfos[i] = &peerInfo{
					serverId:      i,
					index:         len(rf.logs) - 1,
					checkLogsLock: 0,
					channel:       make(chan RequestSyncLogArgs, 20),
					commitChannel: make(chan CommitLogArgs, 20),
				}
			}
		}
		for _, d := range rf.peerInfos {
			d.index = length - 1
		}
		return true
	}
	return false
}

func (rf *Raft) setTerm(term int32) {
	rf.term = term
	rf.persist()
}

func (rf *Raft) appendLog(log *LogEntry) {
	rf.logs = append(rf.logs, log)
	rf.persist()
}

func (rf *Raft) setLog(log *LogEntry, index int) {
	rf.logs[index] = log
	rf.persist()
}
