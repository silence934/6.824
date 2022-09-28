package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) sendRequestVote(server int) bool {
	commitLogTerm := -1
	if int(rf.commitIndex) != -1 {
		commitLogTerm = rf.logs[rf.commitIndex].term
	}
	args := RequestVoteArgs{Term: rf.term, CommitIndex: rf.commitIndex, CommitLogTerm: commitLogTerm, Id: rf.me}

	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	//logger.Debugf("raft[%d]收到投票回应[%d]:%v", rf.me, server, reply.Accept)
	if reply.Accept {
		v := atomic.AddInt32(&rf.vote, 1)
		if int(v) > len(rf.peers)/2 {
			if rf.setRole(candidate, leader) {
				if len(rf.peerInfos) == 0 {
					rf.peerInfos = make([]*peerInfo, len(rf.peers))
					for i := 0; i < len(rf.peerInfos); i++ {
						rf.peerInfos[i] = &peerInfo{serverId: i, index: len(rf.logs) - 1, checkLogsLock: 0, channel: make(chan RequestSyncLogArgs, 20)}
					}
				}
				logger.Infof("raft[%d] 成为leader,term:%d", rf.me, rf.term)
			}
		}
	}

	return ok
}

func (rf *Raft) sendHeartbeat(server int) bool {
	//todo role和term获取尽量是原子获取，否则可能会出现这种情况:
	//旧leader在判断为isLeader=true之后，接收到heartbeat消息，修改term
	//导致旧leader发送的heartbeat消息中的term是现在新的term flower认为这个leader是合法的

	if !rf.isLeader() {
		return false
	}
	term := rf.term
	req := rf.createHeartbeatArgs(len(rf.logs)-1, term)
	resp := RequestHeartbeatReply{}

	ok := rf.peers[server].Call("Raft.Heartbeat", &req, &resp)

	if ok {
		if !resp.Accept {
			return false
		} else if !resp.LogIsAlignment {
			go rf.checkLogs(server, term)
		} else {
			peerIndex := rf.getPeerIndex(server)
			if peerIndex >= 0 && int(rf.commitIndex) < peerIndex {
				log := rf.logs[peerIndex]
				if int(atomic.LoadInt32(&log.syncCount)) > (len(rf.peers) >> 1) {
					go rf.sendCommitLog(log.index, -1)
				}
			}
		}
	}

	logger.Debugf("[heartbeat]  %d----->%d   [resp :%v]", rf.me, server, resp)

	return ok
}

func (rf *Raft) sendCommitLog(commitIndex, server int) {

	log := rf.logs[commitIndex]
	if int32(log.term) != rf.term || int(atomic.LoadInt32(&log.syncCount)) <= (len(rf.peers)>>1) {
		return
	}

	args := CommitLogArgs{Id: rf.me, Term: rf.term, CommitIndex: int32(commitIndex), CommitLogTerm: rf.logs[commitIndex].term}

	if server == -1 {
		for i := range rf.peers {
			reply := CommitLogReply{}
			if i == rf.me {
				go rf.CommitLog(&args, &reply)
			} else {
				atomic.AddInt32(&rf.CommitLogCount, 1)
				go rf.peers[i].Call("Raft.CommitLog", &args, &reply)
			}
		}
	} else {
		reply := CommitLogReply{}
		if server == rf.me {
			go rf.CommitLog(&args, &reply)
		} else {
			atomic.AddInt32(&rf.CommitLogCount, 1)
			go rf.peers[server].Call("Raft.CommitLog", &args, &reply)
		}
	}
}

func (rf *Raft) checkLogs(server int, term int32) {
	if len(rf.peerInfos[server].channel) == 0 && rf.lockCheckLog(server) {
		logger.Infof("leader[%d]开始检查[%d]", rf.me, server)
		defer func() { rf.unlockCheckLog(server) }()

		syncIndex := rf.getPeerIndex(server)
		for true {

			req := rf.createHeartbeatArgs(syncIndex, term)
			resp := RequestHeartbeatReply{}

			ok := rf.peers[server].Call("Raft.CheckLogs", &req, &resp)

			if ok {
				if !resp.Accept {
					//说明对方正在同步日志  下次再检查
					return
				}
				if resp.LogIsAlignment {
					break
				}
				if resp.LogLength > 0 {
					syncIndex = resp.LogLength
				}
			}
			syncIndex--
		}

		length := len(rf.logs)
		logger.Warnf("raft[%d]的日志与当前leader[%d log length=%d]日志最后相同的index=%d!", server, rf.me, length, syncIndex)
		rf.setPeerIndex(server, syncIndex)

		//Args: make([]*RequestSyncLogArgs, length-syncIndex-1)
		req := CoalesceSyncLogArgs{Id: rf.me, Term: rf.term}
		for i := syncIndex + 1; i < length; i++ {
			entry := rf.logs[i]
			args := RequestSyncLogArgs{Index: entry.index, Term: entry.term, Command: entry.command}
			if entry.index != 0 {
				args.PreLogTerm = rf.logs[entry.index-1].term
			}
			req.Args = append(req.Args, &args)
		}
		rf.sendCoalesceSyncLog(server, &req)

	}
}

func (rf *Raft) createHeartbeatArgs(logIndex int, term int32) RequestHeartbeatArgs {
	if logIndex < 0 {
		return RequestHeartbeatArgs{Term: term, LogTerm: -1, LogIndex: -1, CommitIndex: -1, CommitLogTerm: -1}
	}
	log := rf.logs[logIndex]

	commitIndex := int(atomic.LoadInt32(&rf.commitIndex))
	commitLogTerm := -1
	if commitIndex != -1 {
		commitLogTerm = rf.logs[commitIndex].term
	}
	return RequestHeartbeatArgs{
		Term:          rf.term,
		LogTerm:       log.term,
		LogIndex:      log.index,
		CommitIndex:   int32(commitIndex),
		CommitLogTerm: commitLogTerm,
	}
}

func (rf *Raft) sendLogEntry(server int, entry *LogEntry) {
	index := entry.index

	args := RequestSyncLogArgs{Index: index, Term: entry.term, Command: entry.command}
	if index != 0 {
		args.PreLogTerm = rf.logs[index-1].term
	}

	channel := rf.peerInfos[server].channel
	if len(channel) < 19 {
		//容量满了直接丢弃 依赖心跳检测维持一致性
		channel <- args
	}
}

func (rf *Raft) sendLogSuccess(index, server int) {
	log := rf.logs[index]
	count := rf.syncCountAddGet(index)
	rf.setPeerIndex(server, index)

	mid := (len(rf.peers) >> 1) + 1

	if count == mid {
		//第一次同步到过半节点  向所有节点发送提交
		go rf.sendCommitLog(log.index, -1)
	} else if count > mid {
		//后续的可以只发送给这个节点
		go rf.sendCommitLog(log.index, server)
	}
}

func (rf *Raft) sendCoalesceSyncLog(server int, req *CoalesceSyncLogArgs) {

	//保证发送的第一个日志是对方期望的
	expIndex := rf.getPeerIndex(server) + 1
	if len(req.Args) == 0 || expIndex < req.Args[0].Index {
		return
	}

	reply := CoalesceSyncLogReply{}
	ok := rf.peers[server].Call("Raft.CoalesceSyncLog", req, &reply)

	logger.Infof("leader[%d]向raft[%d]节点发送日志,起始index=%d 长度=%d --->对方接受[%v] 长度=%d",
		rf.me, server, req.Args[0].Index, len(req.Args), ok, len(reply.Indexes))

	if ok {
		for _, data := range reply.Indexes {
			rf.sendLogSuccess(*data, server)
		}
	}

}

func (rf *Raft) logEntryLoop() {

	for !rf.killed() {
		if rf.isLeader() {
			for _, peer := range rf.peerInfos {
				if peer.serverId != rf.me && rf.lockCheckLog(peer.serverId) {

					go func(peer *peerInfo) {

						server := peer.serverId
						req := CoalesceSyncLogArgs{Id: rf.me, Term: rf.term}
						for true {
							end := false
							select {
							case syncLogArgs := <-peer.channel:
								req.Args = append(req.Args, &syncLogArgs)
							default:
								end = true
							}
							if end {
								break
							}
						}

						rf.sendCoalesceSyncLog(server, &req)

						rf.unlockCheckLog(peer.serverId)
					}(peer)
				}
			}
		}
		//todo 高并发
		time.Sleep(20 * time.Millisecond)
	}

}
