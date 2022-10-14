package raft

import (
	"fmt"
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

		length := rf.logLength()
		index := args.Index
		rf.logger.Printf(dLog, fmt.Sprintf("hb <--%d %d  ,my length:%d", args.Id, index, length))
		if index < rf.lastIncludedIndex {
			reply.LogIndex = rf.lastIncludedIndex
			reply.LogTerm = rf.lastIncludedTerm
		} else if index >= length {
			//fmt.Printf("%d %d\n", rf.lastIncludedIndex, length)
			reply.LogIndex = length - 1
			reply.LogTerm = rf.entry(length - 1).Term
			//寻找这个term第一次出现的index
			i := length - 1
			for i > rf.lastIncludedIndex && rf.entry(i).Term == reply.LogTerm {
				i--
			}
			if rf.entry(i).Term == reply.LogTerm {
				reply.FirstIndex = i
			} else {
				reply.FirstIndex = i + 1
			}

		} else {

			reply.LogTerm = rf.entry(index).Term
			reply.LogIndex = index
			//寻找这个term第一次出现的index
			i := index
			for i > rf.lastIncludedIndex && rf.entry(i).Term == reply.LogTerm {
				i--
			}
			if rf.entry(i).Term == reply.LogTerm {
				reply.FirstIndex = i
			} else {
				reply.FirstIndex = i + 1
			}

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
		rf.logLength() <= commitIndex || rf.entry(commitIndex).Term != args.CommitLogTerm {

		reply.Accept = false
		return
	}

	rf.lastCallTime = time.Now()

	rf.logger.Printf(dCommit, fmt.Sprintf("commit log=%+v applyIndex:%d", rf.entry(commitIndex), rf.applyIndex))

	rf.flushLog(commitIndex)

	rf.commitIndex = commitIndex
	reply.Accept = true
}

func (rf *Raft) CoalesceSyncLog(req *CoalesceSyncLogArgs, reply *CoalesceSyncLogReply) {
	atomic.AddInt32(&rf.SyncLogEntryCount, 1)

	reply.Indexes = []*int{}
	if !rf.isFollower() || req.Term < rf.term || !rf.lockSyncLog() {
		rf.logger.Printf(dLog, fmt.Sprintf("syncLog failed:%d %d %d", rf.role, rf.term, req.Term))
		return
	}

	defer func() {
		rf.unlockSyncLog()
		rf.logger.Printf(dLog2, fmt.Sprintf("lt startIndex=%d length=%d <--%d   receive=%d",
			req.Args[0].Index, len(req.Args), req.Id, len(reply.Indexes)))
	}()

	rf.lastCallTime = time.Now()

	var lastIndex = 0
	for _, args := range req.Args {
		index := args.Index
		preTerm := args.PreLogTerm
		lastIndex = index

		if rf.logLength() < index || index <= rf.lastIncludedIndex || rf.entry(index-1).Term != preTerm {
			//要追加的日志下标在之前的最后一个日志还要后边：不合法的
			//或者要追加的日志的前一个日志与要追加日志位置的前一个日志的term不相同：不合法
			return
		} else if rf.logLength() == index {
			rf.logs = append(rf.logs, LogEntry{Index: index, Command: args.Command, Term: args.Term})
		} else {
			rf.logs[rf.logIndex(index)] = LogEntry{Index: index, Command: args.Command, Term: args.Term}
			//rf.setLog(&LogEntry{Index: index, Command: args.Command, Term: args.Term}, index)
		}
		reply.Indexes = append(reply.Indexes, &index)
	}

	if rf.logLength() > lastIndex+1 && rf.entry(lastIndex).Term > rf.entry(lastIndex+1).Term {
		//去除多余的日志
		rf.logs = rf.logs[:rf.logIndex(lastIndex+1)]
	}
	rf.persist()

}

func (rf *Raft) AppendLog(req *RequestSyncLogArgs, reply *RequestSyncLogReply) {
	atomic.AddInt32(&rf.SyncLogEntryCount, 1)
	reply.Accept = false

	if !rf.isFollower() || req.Term < int(rf.term) {
		rf.logger.Printf(dError, fmt.Sprintf("appendLog failed:%d %d %d", rf.role, rf.term, req.Term))
		return
	}

	index := req.Index
	preTerm := req.PreLogTerm

	if rf.logLength() < index || index <= rf.lastIncludedIndex || (index != 0 && rf.entry(index-1).Term != preTerm) {
		//要追加的日志下标在之前的最后一个日志还要后边：不合法的
		//或者要追加的日志的前一个日志与要追加日志位置的前一个日志的term不相同：不合法
		return
	} else if rf.logLength() == index {
		rf.appendLog(&LogEntry{Index: index, Command: req.Command, Term: req.Term})
		reply.Accept = true
	} else {
		rf.logs[rf.logIndex(index)] = LogEntry{Index: index, Command: req.Command, Term: req.Term}
		rf.persist()
		reply.Accept = true
	}

	rf.logger.Printf(dLog2, fmt.Sprintf("lt startIndex=%d length=1 <--",
		req.Index))
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	if !rf.isFollower() || args.Term < rf.term || args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.logger.Printf(dError, fmt.Sprintf("IS failed:%d %d %d", rf.role, rf.term, args.Term))
		reply.Accept = false
		return
	}

	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.logger.Printf(dSnap, fmt.Sprintf("IS<--%d index:%d", args.Id, args.LastIncludedIndex))
}
