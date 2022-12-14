package raft

import (
	"fmt"
	"sync/atomic"
	"time"
)

func (rf *Raft) sendRequestVote(server int) bool {
	log := rf.lastEntry()
	args := RequestVoteArgs{
		Id:           rf.me,
		Term:         rf.term,
		LastLogIndex: log.Index,
		LastLogTerm:  log.Term,
	}
	//rf.logger.Printf(dLog, fmt.Sprintf("el --> [%d] ", server))

	reply := RequestVoteReply{}
	ok := rf.call(server, "Raft.RequestVote", &args, &reply)

	if rf.term != args.Term {
		//已经不是本轮选举了
		return false
	}

	rf.logger.Printf(dLog, fmt.Sprintf("vote --> [%d] %v", server, reply))

	if reply.Accept {
		v := atomic.AddInt32(&rf.vote, 1)
		if int(v) > len(rf.peers)/2 {
			if rf.setRole(candidate, leader) {
				rf.initPeerInfos()
				rf.logger.Printf(dLog, fmt.Sprintf("==> leader"))
			}
		}
	}

	return ok
}

func (rf *Raft) sendHeartbeat(server int) bool {

	if !rf.isLeader() || rf.killed() == true {
		return false
	}

	term := rf.term
	peerIndex := rf.getPeerIndex(server)
	peerExpIndex := rf.getPeerExpIndex(server)
	req := RequestHeartbeatArgs{Id: rf.me, Term: term, Index: int(peerExpIndex)}

	resp := RequestHeartbeatReply{}

	startTime := time.Now()
	rf.logger.Printf(dTimer, fmt.Sprintf("hb--->%d %v", server, req.Index))
	ok := rf.call(server, "Raft.Heartbeat", &req, &resp)

	d := time.Now().Sub(startTime)
	if d > rf.heartbeatInterval {
		rf.logger.Printf(dTimer, fmt.Sprintf("hb -->[%d] timeout  %v", server, d))
		//return false
	}
	if ok {
		//每次heartbeat成功就延迟下次heartbeat到来时间
		//rf.peerInfos[server].heartbeatTicker.Reset(rf.heartbeatInterval - d)
	}

	if ok && resp.Accept && rf.isLeader() {
		logIndex := resp.LogIndex
		logTerm := resp.LogTerm

		ok, log := rf.entry(logIndex)
		if !ok {
			rf.logger.Printf(dLog, fmt.Sprintf("hb -->[%d] %v", server, resp.String()))
			//logIndex不在当前日志范围内
			if rf.sendInstallSnapshot(server) && rf.updatePeerIndex(server, peerIndex, rf.lastIncludedIndex) {
				rf.sendHeartbeat(server)
			}
		} else {
			rf.logger.Printf(dLog, fmt.Sprintf("hb -->[%d] %v %v", server, resp.String(), log.String()))
			if log.Term == logTerm {
				//日志匹配 发送logIndex之后的所有日志
				if rf.updatePeerIndex(server, peerIndex, logIndex) {
					rf.sendCoalesceSyncLog(logIndex+1, server, resp.CommitIndex)
				}
			} else if rf.updatePeerExpIndex(server, peerExpIndex, int32(resp.FirstIndex-1)) {
				//日志不匹配  重新检测 不必等到下一次心跳 可以提高日志同步速度
				rf.sendHeartbeat(server)
			}
		}
		return true
	}

	rf.logger.Printf(dTimer, fmt.Sprintf("hb -->[%d] false", server))
	return false
}

func (rf *Raft) sendCoalesceSyncLog(startIndex, server, commitIndex int) {
	length := rf.logLength()

	if length == startIndex {
		//没有日志发送 尝试提交日志 可以解决并发或重启导致没有提交的日志
		rf.sendLogSuccess(startIndex-1, server, commitIndex)
		return
	}

	reply := CoalesceSyncLogReply{}
	ok, req := rf.generateCoalesceLog(startIndex, server)
	if ok == false || len(req.Logs) == 0 {
		return
	}

	ok = rf.call(server, "Raft.CoalesceSyncLog", req, &reply)

	rf.logger.Printf(dLog2, fmt.Sprintf("lt [%d,%d] --> %d receive last index=%d",
		req.Logs[0].Index, req.Logs[len(req.Logs)-1].Index, server, reply.Index))

	if rf.updatePeerIndex(server, startIndex-1, reply.Index) {
		if ok && rf.isLeader() && reply.Index > 0 {
			rf.sendLogSuccess(reply.Index, server, -1)
		}
	} else {
		rf.logger.Printf(dError, fmt.Sprintf("peerIndex has been modified,exp:%d,but it is:%d", startIndex-1, rf.getPeerIndex(server)))
	}
}

func (rf *Raft) sendLogEntry(server int, entry *LogEntry) {

	//保证发送的是对方期望的
	peerIndex := rf.getPeerIndex(server)
	if peerIndex+1 != entry.Index {
		return
	}

	t, pre := rf.entry(entry.Index - 1)
	if !t {
		return
	}
	req := RequestSyncLogArgs{PreLogTerm: pre.Term, Index: entry.Index, Term: entry.Term, Command: entry.Command}
	reply := RequestSyncLogReply{}
	ok := rf.call(server, "Raft.AppendLog", &req, &reply)

	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.logger.Printf(dLog2, fmt.Sprintf("lt index:%d -->%d %v", req.Index, server, reply.Accept))

	if ok && reply.Accept && rf.updatePeerIndex(server, peerIndex, entry.Index) {
		rf.sendLogSuccess(entry.Index, server, -1)
	} else {
		rf.logger.Printf(dError, fmt.Sprintf("peerIndex has been modified,exp:%d,but it is:%d", peerIndex, rf.getPeerIndex(server)))
	}
}

func (rf *Raft) sendCommitLogToBuffer(commitIndex, server int) {

	ok, log := rf.entry(commitIndex)
	if !ok {
		//此时可能已经生成了日志快照
		return
	}
	args := CommitLogArgs{CommitIndex: commitIndex, CommitLogTerm: log.Term}
	if server == rf.me {
		args.Id = rf.me
		args.Term = rf.term
		rf.logger.Printf(dCommit, fmt.Sprintf("send commit -->%d index:%d", server, args.CommitIndex))
		go rf.CommitLog(&args, &CommitLogReply{})
	} else {
		select {
		case rf.peerInfos[server].commitChannel <- &args:
			//rf.logger.Printf(dCommit, fmt.Sprintf("commit to buffer[%d %d]", server, commitIndex))
		default:
		}
	}
}

func (rf *Raft) sendLogSuccess(index, server, commitIndex int) {
	if rf.isLeader() {
		rf.logger.Printf(dTimer, fmt.Sprintf("send log[index:%d] to[%d]  success", index, server))
		if rf.commitIndex >= index {
			if index > commitIndex {
				rf.sendCommitLogToBuffer(index, server)
			}
			return
		}
		ok, log := rf.entry(index)
		if !ok {
			return
		}

		count := 1

		for _, d := range rf.peerInfos {
			if d.index >= index {
				count++
			}
		}
		mid := (len(rf.peers) >> 1) + 1
		//只提交自己term内的日志
		if log.Term == int(rf.term) {
			if count == mid {
				//第一次到达一半的时候，向之前同步完成的节点发送提交请求
				rf.sendCommitLogToBuffer(index, rf.me)
				for _, d := range rf.peerInfos {
					if d.index >= index {
						rf.sendCommitLogToBuffer(log.Index, d.serverId)
					}
				}
			} else if count > mid {
				//后续的可以直接向这个节点发送提交，可能存在并发问题，需要依赖心跳检测补偿
				if rf.commitIndex < index {
					rf.sendCommitLogToBuffer(index, rf.me)
				}
				if index > commitIndex {
					rf.sendCommitLogToBuffer(index, server)
				}
			}
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int) bool {
	rf.logUpdateLock.Lock()
	//不是原子操作 存在并发问题
	args := InstallSnapshotArgs{
		Id:                rf.me,
		Term:              rf.term,
		LastIncludedTerm:  rf.lastIncludedTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		Data:              rf.snapshot,
	}
	rf.logUpdateLock.Unlock()
	reply := InstallSnapshotReply{}

	//r := bytes.NewBuffer(args.Data)
	//d := labgob.NewDecoder(r)
	//var commandIndex int
	//if d.Decode(&commandIndex) != nil {
	//	rf.logger.Errorf("decode error")
	//}

	rf.logger.Printf(dSnap, fmt.Sprintf("sendIS-->%d index:%d ", server, rf.lastIncludedIndex))
	ok := rf.call(server, "Raft.InstallSnapshot", &args, &reply)

	return ok && reply.Accept
}
