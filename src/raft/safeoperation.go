package raft

import (
	"sync/atomic"
)

func (rf *Raft) setPeerExpIndex(server, index int) {
	rf.peerInfos[server].expIndex = index
}

func (rf *Raft) getPeerIndex(server int) int {
	rf.peerInfos[server].updateIndexLock.RLock()
	defer rf.peerInfos[server].updateIndexLock.RUnlock()
	return rf.peerInfos[server].index
}

func (rf *Raft) updatePeerIndex(server, older, new int) bool {
	rf.peerInfos[server].updateIndexLock.Lock()
	defer rf.peerInfos[server].updateIndexLock.Unlock()
	peer := rf.peerInfos[server]
	if peer.index == older {
		peer.index = new
		return true
	}
	return false
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

func (rf *Raft) initPeerInfos() bool {
	if rf.role == candidate && atomic.CompareAndSwapInt32(&rf.initPeers, 0, 1) {
		defer func() { rf.initPeers = 0 }()
		index := rf.lastIncludedIndex
		for _, d := range rf.peerInfos {
			d.index = index
			d.expIndex = rf.logLength() - 1
		}
		rf.startHeartbeatLoop()
		return true
	}
	return false
}

func (rf *Raft) stopHeartbeatLoop() {
	for _, peer := range rf.peerInfos {
		peer.heartbeatTicker.Stop()
	}
}

func (rf *Raft) startHeartbeatLoop() {
	for _, peer := range rf.peerInfos {
		//第一次立刻发送心跳
		peer.heartbeatTicker.Reset(0)
		peer.heartbeatTicker.Reset(rf.heartbeatInterval)
	}
}
