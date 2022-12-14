package raft

import (
	"sync/atomic"
)

func (rf *Raft) updatePeerExpIndex(server int, older, new int32) bool {
	return atomic.CompareAndSwapInt32(&rf.peerInfos[server].expIndex, older, new)
}

func (rf *Raft) getPeerExpIndex(server int) int32 {
	return atomic.LoadInt32(&rf.peerInfos[server].expIndex)
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
		rf.peerInfos[server].expIndex = int32(new)
		return true
	}
	return false
}

func (rf *Raft) setPeerIndex(server, index int) {
	rf.peerInfos[server].updateIndexLock.Lock()
	defer rf.peerInfos[server].updateIndexLock.Unlock()
	rf.peerInfos[server].index = index
}

func (rf *Raft) setRole(old, new int32) bool {
	return atomic.CompareAndSwapInt32(&rf.role, old, new)
}

func (rf *Raft) isLeader() bool {
	return atomic.LoadInt32(&rf.role) == leader
}

func (rf *Raft) isFollower() bool {
	return atomic.LoadInt32(&rf.role) == follower
}

func (rf *Raft) initPeerInfos() {
	index := rf.lastIncludedIndex
	for _, d := range rf.peerInfos {
		d.index = index
		d.expIndex = int32(rf.logLength() - 1)
		d.commitChannel = make(chan *CommitLogArgs, 20)
	}
	rf.startHeartbeatLoop()
}

func (rf *Raft) stopHeartbeatLoop() {
	for _, peer := range rf.peerInfos {
		peer.heartbeatTicker.Stop()
	}
}

func (rf *Raft) startHeartbeatLoop() {
	for _, peer := range rf.peerInfos {
		if rf.me != peer.serverId {
			//第一次立刻发送心跳
			go func(serverId int) {
				rf.sendHeartbeat(serverId)
			}(peer.serverId)
			//peer.heartbeatTicker.Reset(0)
			peer.heartbeatTicker.Reset(rf.heartbeatInterval)
		}
	}
}

func (rf *Raft) call(server int, svcMeth string, args interface{}, reply interface{}) bool {
	for i := 0; i < 2; i++ {
		if rf.peers[server].Call(svcMeth, args, reply) {
			return true
		}
	}
	return false
}
