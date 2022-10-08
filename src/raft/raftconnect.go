package raft

import (
	"fmt"
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

	rf.logger.Printf(dLog, fmt.Sprintf("el <-- [%d] %v", server, reply))

	if reply.Accept {
		v := atomic.AddInt32(&rf.vote, 1)
		if int(v) > len(rf.peers)/2 {
			if rf.initPeerInfos() && rf.setRole(candidate, leader) {
				rf.logger.Printf(dLog, fmt.Sprintf("==> leader"))
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

	if !rf.isLeader() {
		return false
	}

	//rf.logger.Infof("heartbeat--->%d", server)
	term := rf.term
	peerIndex := rf.getPeerIndex(server)
	req := RequestHeartbeatArgs{Id: rf.me, Term: term, Index: peerIndex}

	resp := RequestHeartbeatReply{}

	//startTime := time.Now()
	ok := rf.peers[server].Call("Raft.Heartbeat", &req, &resp)

	if ok && resp.Accept && rf.isLeader() {
		logIndex := resp.LogIndex
		logTerm := resp.LogTerm
		rf.logger.Printf(dLog, fmt.Sprintf("hb -->[%d] %v", server, resp))

		if logIndex == -1 {
			rf.updatePeerIndex(server, peerIndex, logIndex)
			rf.sendLogs(logIndex+1, server)
		} else if rf.logs[logIndex].Term == logTerm {
			rf.updatePeerIndex(server, peerIndex, logIndex)
			rf.sendLogs(logIndex+1, server)
		} else {
			//日志不匹配  重新检测 不必等到下一次检测 可以提高日志同步速度
			rf.updatePeerIndex(server, peerIndex, resp.FirstIndex-1)
			rf.sendHeartbeat(server)
			return true
		}

		return true
	}

	rf.logger.Printf(dTimer, fmt.Sprintf("hb -->[%d] false", server))
	return false
}

func (rf *Raft) sendLogs(startIndex, server int) {
	length := len(rf.logs)
	if length != 0 && length == startIndex {
		rf.sendLogSuccess(startIndex-1, server)
		return
	}

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

	//保证发送的是对方期望的
	peerIndex := rf.getPeerIndex(server)
	if peerIndex+1 != entry.Index {
		return
	}

	req := RequestSyncLogArgs{PreLogTerm: entry.Term, Index: entry.Index, Term: entry.Term, Command: entry.Command}
	reply := RequestSyncLogReply{}
	ok := rf.peers[server].Call("Raft.AppendLog", &req, &reply)
	rf.logger.Printf(dLog2, fmt.Sprintf("lt index:%d -->%d %v", req.Index, server, reply.Accept))

	if ok && reply.Accept && rf.updatePeerIndex(server, peerIndex, entry.Index) {
		rf.sendLogSuccess(entry.Index, server)
	}
}

func (rf *Raft) sendCommitLogToBuffer(commitIndex, server int) {
	//args := CommitLogArgs{Id: rf.me, Term: rf.term, CommitIndex: int32(commitIndex), CommitLogTerm: rf.logs[commitIndex].Term}
	//	go rf.peers[server].Call("Raft.CommitLog", &args, &CommitLogReply{})
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setPeerIndex(server, peerIndex+len(reply.Indexes))

	rf.logger.Printf(dLog2, fmt.Sprintf("lt startIndex=%d length=%d -->%d  %v receive=%d",
		req.Args[0].Index, len(req.Args), server, ok, len(reply.Indexes)))

	if ok && rf.isLeader() {
		for _, data := range reply.Indexes {
			rf.sendLogSuccess(*data, server)
		}
	}
}

func (rf *Raft) sendLogSuccess(index, server int) {
	if rf.isLeader() {
		if int(rf.commitIndex) >= index {
			rf.sendCommitLogToBuffer(index, server)
			return
		}
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
		if log.Term == int(rf.term) {
			if count == mid {
				rf.sendCommitLogToBuffer(index, rf.me)
				for _, d := range rf.peerInfos {
					if d.index >= index {
						rf.sendCommitLogToBuffer(log.Index, d.serverId)
					}
				}
			} else if count > mid {
				if int(rf.commitIndex) < index {
					//并发问题
					rf.sendCommitLogToBuffer(index, rf.me)
				}
				rf.sendCommitLogToBuffer(index, server)
			}
		}
	}
}
