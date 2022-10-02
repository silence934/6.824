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
	//role := rf.getRole()

	reply.Term = rf.term
	if term <= rf.term {
		reply.Accept = false
	} else {
		rf.voteLock.Lock()
		defer rf.voteLock.Unlock()
		if term <= rf.term {
			reply.Accept = false
		} else {
			//term 和isLeader不可交换顺序
			rf.role = follower
			rf.setTerm(term)
			if rf.acceptVote(args) {
				rf.lastCallTime = time.Now()
				reply.Accept = true
			} else {
				reply.Accept = false
			}
		}
	}

	//logger.Infof("raft[%d  %d]收到投票请求对方[%d]的term:%d 结果=%v,CommitIndex:%d,我的term:%d CommitIndex:%d", rf.me, rf.role, args.Id, args.Term, reply.Accept, args.CommitIndex, rf.Term, rf.commitIndex)
	//logger.Debugf("raft[%d %d]收到投票请求[%d]  结果=%v", rf.me, role, args.Id, reply.Accept)

}

func (rf *Raft) Heartbeat(args *RequestHeartbeatArgs, reply *RequestHeartbeatReply) {
	atomic.AddInt32(&rf.HeartbeatCount, 1)
	term := rf.term
	if term <= args.Term {
		rf.lastCallTime = time.Now()
		rf.role = follower
		rf.setTerm(args.Term)

		reply.Accept = true

		length := len(rf.logs)
		index := args.Index
		//logger.Infof("%d-->%d  %d  %d", args.Id, rf.me, index, length)
		if index == -1 || length == 0 {
			reply.LogIndex = -1
			reply.LogTerm = -1
		} else if index >= length {
			reply.LogIndex = length - 1
			reply.LogTerm = rf.logs[length-1].Term
			i := length - 1
			for i > -1 && rf.logs[i].Term == reply.LogTerm {
				i--
			}
			reply.FirstIndex = i + 1
		} else {
			reply.LogTerm = rf.logs[index].Term
			reply.LogIndex = index
			i := index
			for i > -1 && rf.logs[i].Term == reply.LogTerm {
				i--
			}
			reply.FirstIndex = i + 1
		}

	} else {
		reply.Accept = false
	}
	//logger.Debugf("[heartbeat] [%d %d] -----> [%d %d]  resp:%v", args.Id, args.Term, rf.me, term, reply.Accept)

}

func (rf *Raft) CommitLog(args *CommitLogArgs, reply *CommitLogReply) {
	atomic.AddInt32(&rf.CommitLogCount, 1)

	commitIndex := args.CommitIndex
	term := rf.term
	currentCommitIndex := rf.commitIndex

	if term > args.Term || currentCommitIndex >= commitIndex ||
		len(rf.logs) <= int(commitIndex) || rf.logs[commitIndex].Term != args.CommitLogTerm {

		reply.Accept = false
		return
	}

	rf.lastCallTime = time.Now()

	rf.commitIndex = commitIndex
	logger.Infof("[commit log] %d  Index=%d ----->", rf.me, commitIndex)
	rf.flushLog(int(commitIndex))
	reply.Accept = true
}

func (rf *Raft) CoalesceSyncLog(req *CoalesceSyncLogArgs, reply *CoalesceSyncLogReply) {
	atomic.AddInt32(&rf.SyncLogEntryCount, 1)

	reply.Indexes = []*int{}
	if !rf.isFollower() || req.Term < rf.term || !rf.lockSyncLog() {
		logger.Infof("%d %d %d", rf.role, rf.term, req.Term)
		return
	}

	defer func() {
		rf.unlockSyncLog()
		if err := recover(); err != nil {
			logger.Errorf("出现错误:  原logs 长度=%d,\n接收到的数据:%v", len(rf.logs), req.Args)
			panic(err)
		}
	}()

	rf.lastCallTime = time.Now()

	for _, args := range req.Args {
		index := args.Index
		preTerm := args.PreLogTerm

		if len(rf.logs) < index || (index != 0 && rf.logs[index-1].Term != preTerm) {
			//要追加的日志下标在之前的最后一个日志还要后边：不合法的
			//或者要追加的日志的前一个日志与要追加日志位置的前一个日志的term不相同：不合法
			return
		} else if len(rf.logs) == index {
			//complete := make([]bool, len(rf.peers))
			//complete[rf.me] = true
			rf.appendLog(&LogEntry{Index: index, Command: args.Command, Term: args.Term})
		} else {
			//complete := make([]bool, len(rf.peers))
			//complete[rf.me] = true
			rf.setLog(&LogEntry{Index: index, Command: args.Command, Term: args.Term}, index)
		}
		reply.Indexes = append(reply.Indexes, &index)
	}

}
