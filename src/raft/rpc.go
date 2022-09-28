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

func (rf *Raft) CheckLogs(args *RequestHeartbeatArgs, reply *RequestHeartbeatReply) {
	atomic.AddInt32(&rf.HeartbeatCount, 1)

	if !rf.isFollower() && !rf.lockSyncLog() {
		reply.Accept = false
		return
	}
	defer rf.unlockSyncLog()

	// Your code here (2A, 2B).
	if rf.term <= args.Term {
		rf.lastCallTime = time.Now()
		rf.role = follower
		rf.term = args.Term

		logIndex := args.LogIndex
		logTerm := args.LogTerm
		reply.Accept = true

		logger.Infof("我方[%d]日志与对方比较  我方长度:%d  对方index:%d", rf.me, len(rf.logs), logIndex)

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

	if !rf.isFollower() || !rf.lockSyncLog() {
		reply.Accept = false
		return
	}
	defer rf.unlockSyncLog()

	rf.lastCallTime = time.Now()

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

func (rf *Raft) CoalesceSyncLog(req *CoalesceSyncLogArgs, reply *CoalesceSyncLogReply) {
	atomic.AddInt32(&rf.SyncLogEntryCount, 1)

	//logger.Infof("raft[%d]收到[%d]日志添加请求 leng=%d", rf.me, req.Id, len(req.Args))

	reply.Indexes = []*int{}
	if !rf.isFollower() || req.Term < rf.term || !rf.lockSyncLog() {
		return
	}
	defer rf.unlockSyncLog()

	rf.lastCallTime = time.Now()

	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("出现错误:  原logs 长度=%d,\n接收到的数据:%v", len(rf.logs), req.Args)
			panic(err)
		}
	}()

	for _, args := range req.Args {
		index := args.Index
		preTerm := args.PreLogTerm

		if len(rf.logs) < index || (index != 0 && rf.logs[index-1].term != preTerm) {
			//要追加的日志下标在之前的最后一个日志还要后边：不合法的
			//或者要追加的日志的前一个日志与要追加日志位置的前一个日志的term不相同：不合法
			return
		} else if len(rf.logs) == index {
			rf.logs = append(rf.logs, &LogEntry{index: index, command: args.Command, term: args.Term})
		} else {
			rf.logs[index] = &LogEntry{index: index, command: args.Command, term: args.Term}
		}
		reply.Indexes = append(reply.Indexes, &index)
	}

}
