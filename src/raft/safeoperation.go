package raft

import (
	"sync"
	"sync/atomic"
)

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

func (rf *Raft) initPeerInfos() bool {
	if atomic.CompareAndSwapInt32(&rf.initPeers, 0, 1) {
		defer func() { rf.initPeers = 0 }()
		length := rf.logLength()
		if len(rf.peerInfos) == 0 {
			rf.peerInfos = make([]*peerInfo, len(rf.peers))
			for i := 0; i < len(rf.peerInfos); i++ {
				rf.peerInfos[i] = &peerInfo{
					serverId:        i,
					index:           rf.logLength() - 1,
					updateIndexLock: &sync.RWMutex{},
					channel:         make(chan RequestSyncLogArgs, 20),
					commitChannel:   make(chan *CommitLogArgs, 20),
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
