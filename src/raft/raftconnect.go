package raft

import (
	"sync/atomic"
)

func (rf *Raft) sendRequestVote(server int) bool {

	length := len(rf.logs)
	lastLogTerm := -1

	if length > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].term
	}
	args := RequestVoteArgs{
		Id:          rf.me,
		Term:        rf.term,
		LogsLength:  length,
		LastLogTerm: lastLogTerm,
	}

	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	logger.Infof("[election] %d -----> %d  resp:%v", rf.me, server, reply.Accept)
	if reply.Accept {
		v := atomic.AddInt32(&rf.vote, 1)
		if int(v) > len(rf.peers)/2 {
			rf.initPeerInfos()
			if rf.setRole(candidate, leader) {
				logger.Infof("raft[%d] ==> leader,term:%d", rf.me, rf.term)
			}
		}
	}

	return ok
}

func (rf *Raft) sendHeartbeat(server int) bool {
	//todo role和term获取尽量是原子获取，否则可能会出现这种情况:
	//旧leader在判断为isLeader=true之后，接收到heartbeat消息，修改term
	//导致旧leader发送的heartbeat消息中的term是现在新的term flower认为这个leader是合法的

	if !rf.isLeader() || !rf.lockCheckLog(server) {
		return false
	}
	defer rf.unlockCheckLog(server)

	term := rf.term
	req := RequestHeartbeatArgs{Id: rf.me, Term: term, Index: rf.getPeerIndex(server)}

	//rf.createHeartbeatArgs(len(rf.logs)-1, term)
	resp := RequestHeartbeatReply{}

	ok := rf.peers[server].Call("Raft.Heartbeat", &req, &resp)

	if ok {
		if resp.Accept {
			logIndex := resp.LogIndex
			logTerm := resp.LogTerm
			logger.Debugf("[heartbeat]  %d----->%d   resp :%v", rf.me, server, resp)

			if logIndex == -1 {
				rf.setPeerIndex(server, logIndex)
				rf.sendLogs(logIndex+1, server)
			} else if rf.logs[logIndex].term == logTerm {
				rf.setPeerIndex(server, logIndex)
				rf.sendLogs(logIndex+1, server)
			} else {
				//日志不匹配  重新检测心跳不必等到下一次检测 可以提高日志同步速度
				rf.setPeerIndex(server, logIndex-1)
				rf.unlockCheckLog(server)
				rf.sendHeartbeat(server)
			}
			return true
		}
	}

	logger.Debugf("[heartbeat]  %d----->%d   resp :%v", rf.me, server, resp)
	return false
}

func (rf *Raft) sendLogs(startIndex, server int) {
	length := len(rf.logs)
	req := CoalesceSyncLogArgs{Id: rf.me, Term: rf.term}
	for i := startIndex; i < length; i++ {
		entry := rf.logs[i]
		args := RequestSyncLogArgs{Index: entry.index, Term: entry.term, Command: entry.command}
		if entry.index != 0 {
			args.PreLogTerm = rf.logs[entry.index-1].term
		}
		req.Args = append(req.Args, &args)
	}
	rf.sendCoalesceSyncLog(server, &req)
}

func (rf *Raft) sendLogEntryToBuffer(server int, entry *LogEntry) {
	index := entry.index

	args := RequestSyncLogArgs{Index: index, Term: entry.term, Command: entry.command}
	if index != 0 {
		args.PreLogTerm = rf.logs[index-1].term
	}

	select {
	case rf.peerInfos[server].channel <- args:
		//logger.Debugf("xxxxxx: %d %d %d", rf.me, server, len(rf.peerInfos[server].channel))
	default:
		//容量满了直接丢弃 依赖心跳检测维持一致性
	}
}

func (rf *Raft) sendCommitLogToBuffer(commitIndex, server int) {

	log := rf.logs[commitIndex]
	if int32(log.term) != rf.term || int(atomic.LoadInt32(&log.syncCount)) <= (len(rf.peers)>>1) {
		return
	}
	args := CommitLogArgs{CommitIndex: int32(commitIndex), CommitLogTerm: rf.logs[commitIndex].term}

	if server == -1 {
		for _, peer := range rf.peerInfos {
			select {
			case peer.commitChannel <- args:
			default:
			}
		}
	} else {
		select {
		case rf.peerInfos[server].commitChannel <- args:
		default:
		}
	}
}

func (rf *Raft) sendLogSuccess(index, server int) {
	log := rf.logs[index]
	count := rf.syncCountAddGet(index)
	rf.setPeerIndex(server, index)

	mid := (len(rf.peers) >> 1) + 1

	if count == mid {
		rf.sendCommitLogToBuffer(index, rf.me)
		for _, d := range rf.peerInfos {
			if d.index >= index {
				rf.sendCommitLogToBuffer(log.index, d.serverId)
			}
		}
	} else if count > mid || int(rf.commitIndex) >= index {
		//后续的可以只发送给这个节点
		rf.sendCommitLogToBuffer(index, server)
	}
}

func (rf *Raft) sendCoalesceSyncLog(server int, req *CoalesceSyncLogArgs) {

	//保证发送的第一个日志是对方期望的
	expIndex := rf.getPeerIndex(server) + 1
	if len(req.Args) == 0 || expIndex != req.Args[0].Index {
		return
	}

	reply := CoalesceSyncLogReply{}
	ok := rf.peers[server].Call("Raft.CoalesceSyncLog", req, &reply)

	logger.Infof("[log transfer] %d----->%d [%v startIndex=%d length=%d receive=%d]",
		rf.me, server, ok, req.Args[0].Index, len(req.Args), len(reply.Indexes))

	if ok {
		for _, data := range reply.Indexes {
			rf.sendLogSuccess(*data, server)
		}
	}

}
