package raft

import (
	"sync/atomic"
)

func (rf *Raft) getPeerExpIndex(server int) int {
	rf.peerInfos[server].updateIndexLock.RLock()
	defer rf.peerInfos[server].updateIndexLock.RUnlock()
	return rf.peerInfos[server].expIndex
}

func (rf *Raft) getPeerMatchIndex(server int) int {
	//更新match index和 exp index 不可以同时进行
	rf.peerInfos[server].updateIndexLock.RLock()
	defer rf.peerInfos[server].updateIndexLock.RUnlock()
	return rf.peerInfos[server].matchIndex
}

func (rf *Raft) updatePeerIndex(server, older, new int) bool {
	//更新match index和 exp index 不可以同时进行
	rf.peerInfos[server].updateIndexLock.Lock()
	defer rf.peerInfos[server].updateIndexLock.Unlock()
	peer := rf.peerInfos[server]
	if peer.matchIndex == older {
		peer.matchIndex = new
		peer.expIndex = new
		return true
	}
	return false
}

func (rf *Raft) setPeerIndex(server, index int) {
	rf.peerInfos[server].updateIndexLock.Lock()
	defer rf.peerInfos[server].updateIndexLock.Unlock()
	rf.peerInfos[server].matchIndex = index
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
	matchIndex := rf.lastIncludedIndex
	expIndex := rf.logLength() - 1
	for _, d := range rf.peerInfos {
		d.matchIndex = matchIndex
		d.expIndex = expIndex
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
