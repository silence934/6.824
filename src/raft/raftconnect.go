package raft

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)

func (rf *Raft) sendRequestVote(server int) bool {
	log := rf.lastEntry()
	args := RequestVoteArgs{
		Id:          rf.me,
		Term:        rf.term,
		LogsLength:  log.Index + 1,
		LastLogTerm: log.Term,
	}
	//rf.logger.Printf(dLog, fmt.Sprintf("el --> [%d] ", server))

	t := time.Now()
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	if time.Now().Sub(t).Milliseconds() > 150 {
		return false
	}

	rf.logger.Printf(dLog, fmt.Sprintf("vote --> [%d] %v", server, reply))

	if reply.Accept {
		v := atomic.AddInt32(&rf.vote, 1)
		if int(v) > len(rf.peers)/2 {
			if rf.initPeerInfos() && rf.setRole(candidate, leader) {
				rf.logger.Printf(dLog, fmt.Sprintf("==> leader"))
			}
		}
	}

	return ok
}

func (rf *Raft) sendHeartbeat(server, index int) bool {

	if !rf.isLeader() {
		return false
	}

	term := rf.term
	peerIndex := rf.getPeerIndex(server)
	req := RequestHeartbeatArgs{Id: rf.me, Term: term, Index: index}

	resp := RequestHeartbeatReply{}

	//rf.logger.Infof("heartbeat--->%d", server)
	//startTime := time.Now()
	ok := rf.peers[server].Call("Raft.Heartbeat", &req, &resp)
	if !ok {
		//快速重试
		ok = rf.peers[server].Call("Raft.Heartbeat", &req, &resp)
	}

	if ok && resp.Accept && rf.isLeader() {
		logIndex := resp.LogIndex
		logTerm := resp.LogTerm

		ok, log := rf.entry(logIndex)
		rf.logger.Printf(dLog, fmt.Sprintf("hb -->[%d] %v %v", server, resp, log))
		if !ok {
			//logIndex不在当前日志范围内
			if rf.sendInstallSnapshot(server) && rf.updatePeerIndex(server, peerIndex, rf.lastIncludedIndex) {
				rf.sendHeartbeat(server, rf.lastIncludedIndex)
			}
		} else if log.Term == logTerm {
			//日志匹配 发送logIndex之后的所有日志
			if rf.updatePeerIndex(server, peerIndex, logIndex) {
				rf.sendCoalesceSyncLog(logIndex+1, server, resp.CommitIndex)
			}
		} else {
			//if rf.updatePeerIndex(server, peerIndex, resp.FirstIndex-1)
			//日志不匹配  重新检测 不必等到下一次心跳 可以提高日志同步速度
			rf.sendHeartbeat(server, resp.FirstIndex-1)
			return true
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
		if startIndex-1 > commitIndex {
			rf.sendLogSuccess(startIndex-1, server)
		}
		return
	}
	//保证发送的第一个日志是对方期望的
	peerIndex := rf.getPeerIndex(server)
	ok, firstLog := rf.entry(startIndex)
	if !ok {
		return
	}

	if peerIndex+1 != firstLog.Index {
		rf.logger.Printf(dError, fmt.Sprintf("sendCoalesceSyncLog failed,ratf[%d] exp:%d ,bug first:%d", server, peerIndex+1, startIndex))
		return
	}

	ok, preLog := rf.entry(startIndex - 1)
	if !ok {
		return
	}

	req := CoalesceSyncLogArgs{Id: rf.me, Term: rf.term, Logs: []*RequestSyncLogArgs{{Index: firstLog.Index, Term: firstLog.Term, Command: firstLog.Command, PreLogTerm: preLog.Term}}}
	preLog = firstLog

	for i := startIndex + 1; i < length; i++ {
		ok, log := rf.entry(i)
		if ok {
			req.Logs = append(req.Logs, &RequestSyncLogArgs{Index: log.Index, Term: log.Term, Command: log.Command, PreLogTerm: preLog.Term})
		} else {
			return
		}
		preLog = log
	}

	reply := CoalesceSyncLogReply{}
	ok = rf.peers[server].Call("Raft.CoalesceSyncLog", &req, &reply)

	//todo 意义?
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.updatePeerIndex(server, peerIndex, peerIndex+len(reply.Indexes)) {
		rf.logger.Printf(dLog2, fmt.Sprintf("lt startIndex=%d length=%d -->%d  %v receive=%d",
			req.Logs[0].Index, len(req.Logs), server, ok, len(reply.Indexes)))

		if ok && rf.isLeader() {
			for _, data := range reply.Indexes {
				rf.sendLogSuccess(*data, server)
			}
		}
	} else {
		rf.logger.Printf(dError, fmt.Sprintf("peerIndex has been modified,exp:%d,but it is:%d", peerIndex, rf.getPeerIndex(server)))
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
	ok := rf.peers[server].Call("Raft.AppendLog", &req, &reply)

	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.logger.Printf(dLog2, fmt.Sprintf("lt index:%d -->%d %v", req.Index, server, reply.Accept))

	if ok && reply.Accept && rf.updatePeerIndex(server, peerIndex, entry.Index) {
		rf.sendLogSuccess(entry.Index, server)
	}
}

func (rf *Raft) sendCommitLogToBuffer(commitIndex, server int) {
	//args := CommitLogArgs{Id: rf.me, Term: rf.term, CommitIndex: int32(commitIndex), CommitLogTerm: rf.logs[commitIndex].Term}
	//	go rf.peers[server].Call("Raft.CommitLog", &args, &CommitLogReply{})
	ok, log := rf.entry(commitIndex)
	if !ok {
		//此时可能已经生成了日志快照
		return
	}
	args := CommitLogArgs{CommitIndex: commitIndex, CommitLogTerm: log.Term}
	if server == -1 {
		for _, peer := range rf.peerInfos {
			select {
			case peer.commitChannel <- &args:
			default:
			}
		}
	} else {
		select {
		case rf.peerInfos[server].commitChannel <- &args:
		default:
		}
	}
}

func (rf *Raft) sendLogSuccess(index, server int) {
	if rf.isLeader() {
		if rf.commitIndex >= index {
			rf.sendCommitLogToBuffer(index, server)
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
				rf.sendCommitLogToBuffer(index, server)
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

	//todo 待删除
	r := bytes.NewBuffer(args.Data)
	d := labgob.NewDecoder(r)
	var commandIndex int
	if d.Decode(&commandIndex) != nil {
		rf.logger.Errorf("decode error")
	}

	rf.logger.Printf(dSnap, fmt.Sprintf("sendIS-->%d index:%d snapshotIndex:%d", server, rf.lastIncludedIndex, commandIndex))
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	return ok && reply.Accept
}
