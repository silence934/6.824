package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) sendRequestVote(server int) bool {

	length := len(rf.logs)
	lastLogTerm := -1

	if length > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	args := RequestVoteArgs{
		Id:          rf.me,
		Term:        rf.term,
		LogsLength:  length,
		LastLogTerm: lastLogTerm,
	}

	t := time.Now()
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	if time.Now().Sub(t).Milliseconds() > 150 {
		return false
	}

	logger.Infof("[election] [%d %d] -----> [%d]  resp:%v  time-consuming:%v", rf.me, args.Term, server, reply, time.Now().Sub(t))

	if reply.Accept {
		v := atomic.AddInt32(&rf.vote, 1)
		if int(v) > len(rf.peers)/2 {
			if rf.initPeerInfos() && rf.setRole(candidate, leader) {
				logger.Infof("raft[%d] ==> leader,Term:%d", rf.me, rf.term)
				//for i, _ := range rf.peers {
				//	if i != rf.me {
				//		go rf.sendHeartbeat(i)
				//	}
				//}
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

	//lock := rf.lockCheckLog(server, 150*time.Millisecond)
	//if !lock.Lock() {
	//	return false
	//}
	//defer lock.Unlock()
	logger.Infof("[heartbeat]  %d----->%d", rf.me, server)
	term := rf.term
	req := RequestHeartbeatArgs{Id: rf.me, Term: term, Index: rf.getPeerIndex(server)}

	resp := RequestHeartbeatReply{}

	//startTime := time.Now()
	ok := rf.peers[server].Call("Raft.Heartbeat", &req, &resp)
	//if lock.TimeOut() {
	//	logger.Infof("[heartbeat]  %d----->%d   timeout: %v", rf.me, server, time.Now().Sub(startTime))
	//	return false
	//}

	if ok {
		if resp.Accept {
			logIndex := resp.LogIndex
			logTerm := resp.LogTerm
			logger.Infof("[heartbeat]  %d----->%d   resp :%v", rf.me, server, resp)

			if logIndex == -1 {
				rf.setPeerIndex(server, logIndex)
				rf.sendLogs(logIndex+1, server)
			} else if rf.logs[logIndex].Term == logTerm {
				rf.setPeerIndex(server, logIndex)
				rf.sendLogs(logIndex+1, server)
			} else {
				//日志不匹配  重新检测 不必等到下一次检测 可以提高日志同步速度
				rf.setPeerIndex(server, resp.FirstIndex-1)
				rf.sendHeartbeat(server)
				return true
			}

			return true
		}
	}

	logger.Debugf("[heartbeat]  %d----->%d   resp :false", rf.me, server)
	return false
}

func (rf *Raft) sendLogs(startIndex, server int) {
	length := len(rf.logs)
	req := CoalesceSyncLogArgs{Id: rf.me, Term: rf.term}
	for i := startIndex; i < length; i++ {
		entry := rf.logs[i]
		args := RequestSyncLogArgs{Index: entry.Index, Term: entry.Term, Command: entry.Command}
		if entry.Index != 0 {
			args.PreLogTerm = rf.logs[entry.Index-1].Term
		}
		req.Args = append(req.Args, &args)
	}
	rf.sendCoalesceSyncLog(server, &req)
}

func (rf *Raft) sendLogEntryToBuffer(server int, entry *LogEntry) {
	index := entry.Index

	args := RequestSyncLogArgs{Index: index, Term: entry.Term, Command: entry.Command}
	if index != 0 {
		args.PreLogTerm = rf.logs[index-1].Term
	}

	select {
	case rf.peerInfos[server].channel <- args:
		//logger.Debugf("xxxxxx: %d %d %d", rf.me, server, len(rf.peerInfos[server].channel))
	default:
		//容量满了直接丢弃 依赖心跳检测维持一致性
	}
}

func (rf *Raft) sendCommitLogToBuffer(commitIndex, server int) {
	args := CommitLogArgs{CommitIndex: int32(commitIndex), CommitLogTerm: rf.logs[commitIndex].Term}
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

func (rf *Raft) sendCoalesceSyncLog(server int, req *CoalesceSyncLogArgs) {

	//保证发送的第一个日志是对方期望的
	peerIndex := rf.getPeerIndex(server)
	if len(req.Args) == 0 || peerIndex+1 != req.Args[0].Index {
		return
	}

	reply := CoalesceSyncLogReply{}
	ok := rf.peers[server].Call("Raft.CoalesceSyncLog", req, &reply)
	//if lock.TimeOut() {
	//	logger.Infof("[log transfer timeout] %d----->%d  [%v startIndex=%d length=%d receive=%d]",
	//		rf.me, server, ok, req.Args[0].Index, len(req.Args), len(reply.Indexes))
	//	return
	//}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setPeerIndex(server, peerIndex+len(reply.Indexes))

	logger.Infof("[log transfer] %d----->%d  [%v startIndex=%d length=%d receive=%d]",
		rf.me, server, ok, req.Args[0].Index, len(req.Args), len(reply.Indexes))

	if ok {
		for _, data := range reply.Indexes {
			rf.sendLogSuccess(*data, server)
		}
	}
}

func (rf *Raft) sendLogSuccess(index, server int) {
	log := rf.logs[index]
	//log.Complete[server] = true
	//count := rf.syncCountAddGet(index)
	count := 1

	for _, d := range rf.peerInfos {
		if d.index >= index {
			count++
		}
	}

	mid := (len(rf.peers) >> 1) + 1
	if int(rf.commitIndex) >= index {
		rf.sendCommitLogToBuffer(index, server)
	} else if log.Term == int(rf.term) {
		if count == mid {
			rf.sendCommitLogToBuffer(index, rf.me)
			for _, d := range rf.peerInfos {
				if d.index >= index {
					rf.sendCommitLogToBuffer(log.Index, d.serverId)
				}
			}
		} else if count > mid {
			rf.sendCommitLogToBuffer(index, server)
		}
	}
}
