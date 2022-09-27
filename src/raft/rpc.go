package raft

import (
	"sync/atomic"
	"time"
)

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	atomic.AddInt32(&rf.RequestVoteCount, 1)

	term := args.Term
	role := rf.getRole()

	if term < rf.term {
		reply.Accept = false
	} else {
		rf.term = term
		//		if rf.accept(args.CommitIndex, args.CommitLogTerm) && (rf.setRole(follower, follower) || rf.setRole(leader, follower)) {
		//&& !(atomic.CompareAndSwapInt32(&rf.role, follower, candidate) || atomic.CompareAndSwapInt32(&rf.role, leader, candidate))
		if rf.accept(args.CommitIndex, args.CommitLogTerm) && !(atomic.CompareAndSwapInt32(&rf.role, follower, candidate) || atomic.CompareAndSwapInt32(&rf.role, leader, candidate)) {
			rf.lastCallTime = time.Now()
			rf.role = follower
			reply.Accept = true
		} else {
			reply.Accept = false
		}
	}

	//logger.Infof("raft[%d  %d]收到投票请求对方[%d]的term:%d 结果=%v,CommitIndex:%d,我的term:%d CommitIndex:%d", rf.me, rf.role, args.Id, args.Term, reply.Accept, args.CommitIndex, rf.term, rf.commitIndex)
	logger.Infof("raft[%d %d]收到投票请求[%d]  结果=%v", rf.me, role, args.Id, reply.Accept)

}

func (rf *Raft) Heartbeat(args *RequestHeartbeatArgs, reply *RequestHeartbeatReply) {
	atomic.AddInt32(&rf.HeartbeatCount, 1)

	// Your code here (2A, 2B).
	if rf.term <= args.Term {
		rf.lastCallTime = time.Now()
		rf.role = follower
		rf.term = args.Term

		logIndex := args.LogIndex
		logTerm := args.LogTerm
		reply.Accept = true

		if logIndex == -1 {
			reply.LogIsAlignment = len(rf.logs) == 0
		} else if logIndex >= len(rf.logs) {
			reply.LogIsAlignment = false
			reply.LogLength = len(rf.logs)
		} else if rf.logs[logIndex].term == logTerm {
			reply.LogIsAlignment = true
			rf.commitIndex = args.CommitIndex
		} else {
			reply.LogIsAlignment = false
		}
	} else {
		reply.Accept = false
	}

}

//提交的条件：
//1.日志同步次数超过raft数量的一半
//2.这个日志是最后一条日志
//3.这个日志的term是当前term
func (rf *Raft) CommitLog(args *CommitLogArgs, reply *CommitLogReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	commitIndex := args.CommitIndex
	term := rf.term
	currentCommitIndex := rf.commitIndex

	if term > args.Term || currentCommitIndex >= commitIndex ||
		len(rf.logs) <= int(commitIndex) || rf.logs[commitIndex].term != args.CommitLogTerm {

		reply.Accept = false
		return
	}

	rf.lastCallTime = time.Now()

	rf.commitIndex = commitIndex
	logger.Infof("raft[%d]提交了日志,当前commitIndex=%d", rf.me, commitIndex)
	rf.flushLog(int(commitIndex))
	reply.Accept = true
}

func (rf *Raft) SyncLogEntry(args *RequestSyncLogArgs, reply *RequestSyncLogReply) {
	atomic.AddInt32(&rf.SyncLogEntryCount, 1)

	if !rf.isFollower() {
		reply.Accept = false
		return
	}
	rf.lastCallTime = time.Now()
	rf.syncLogLock.Lock()
	defer rf.syncLogLock.Unlock()
	length := len(rf.logs)
	index := args.Index
	preTerm := args.PreLogTerm
	reply.Accept = true
	if length < index || (index != 0 && rf.logs[index-1].term != preTerm) {
		reply.Accept = false
	} else if length == index {
		rf.logs = append(rf.logs, &LogEntry{index: index, command: args.Command, term: args.Term})
		//rf.applyCh <- ApplyMsg{CommandValid: true, Command: args.Command, CommandIndex: index + 1}
	} else {
		rf.logs[index] = &LogEntry{index: index, command: args.Command, term: args.Term}
	}

}
