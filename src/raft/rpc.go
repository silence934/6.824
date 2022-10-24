package raft

import (
	"fmt"
	"sync/atomic"
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
				rf.updateLastTime()
				rf.stopHeartbeatLoop()
				reply.Accept = true
			} else {
				reply.Accept = false
			}
		}
	}
	rf.logger.Printf(dLog, fmt.Sprintf("vote <-- [%d] %v", args.Id, *reply))

	//logger.Infof("raft[%d  %d]收到投票请求对方[%d]的term:%d 结果=%v,CommitIndex:%d,我的term:%d CommitIndex:%d", rf.me, rf.role, args.Id, args.Term, reply.Accept, args.CommitIndex, rf.Term, rf.commitIndex)
	//logger.Debugf("raft[%d %d]收到投票请求[%d]  结果=%v", rf.me, role, args.Id, reply.Accept)

}

func (rf *Raft) Heartbeat(args *RequestHeartbeatArgs, reply *RequestHeartbeatReply) {
	atomic.AddInt32(&rf.HeartbeatCount, 1)
	term := rf.term
	//reply.RespTime = time.Now().UnixMilli()
	if term <= args.Term {
		rf.updateLastTime()
		rf.role = follower
		rf.setTerm(args.Term)

		reply.Accept = true
		reply.CommitIndex = rf.commitIndex

		length := rf.logLength()
		reqIndex := args.Index
		rf.logger.Printf(dLog, fmt.Sprintf("hb <-- id:%d index:%d  ,my length:%d", args.Id, reqIndex, length))
		ok, log := rf.entry(reqIndex)
		if ok {
			reply.LogTerm = log.Term
			reply.LogIndex = reqIndex
			//寻找这个term第一次出现的index
			i := reqIndex
			for true {
				if i <= rf.lastIncludedIndex {
					break
				}
				o, l := rf.entry(i - 1)
				if o && l.Term == log.Term {
					i--
				} else {
					break
				}
			}
			reply.FirstIndex = i
		} else {
			if reqIndex < rf.lastIncludedIndex {
				reply.LogIndex = rf.lastIncludedIndex
				reply.FirstIndex = rf.lastIncludedIndex
				reply.LogTerm = rf.lastIncludedTerm
			} else if reqIndex >= length {
				log := rf.lastEntry()
				reply.LogIndex = log.Index
				reply.LogTerm = log.Term
				//寻找这个term第一次出现的index
				i := log.Index
				for true {
					if i <= rf.lastIncludedIndex {
						break
					}
					o, l := rf.entry(i - 1)
					if o && l.Term == log.Term {
						i--
					} else {
						break
					}
				}
				reply.FirstIndex = i
			}
		}

	} else {
		reply.Accept = false
		rf.logger.Printf(dTimer, fmt.Sprintf("hb <-- id:%d term:%d false", args.Id, args.Term))
	}
	//logger.Debugf("[heartbeat] [%d %d] -----> [%d %d]  resp:%v", args.Id, args.Term, rf.me, term, reply.Accept)

}

func (rf *Raft) CommitLog(args *CommitLogArgs, reply *CommitLogReply) {
	atomic.AddInt32(&rf.CommitLogCount, 1)
	rf.commitLogLock.Lock()
	defer rf.commitLogLock.Unlock()

	commitIndex := args.CommitIndex
	term := rf.term
	currentCommitIndex := rf.commitIndex

	ok, log := rf.entry(commitIndex)
	if term > args.Term || //节点term不合法
		currentCommitIndex >= commitIndex || //之前已提交过
		!ok || //不在范围内
		log.Term != args.CommitLogTerm { //日志term不合法

		reply.Accept = false
		return
	}

	rf.updateLastTime()

	rf.logger.Printf(dCommit, fmt.Sprintf("commit log Index:%+v applyIndex:%d", log.Index, rf.applyIndex))

	rf.flushLog(commitIndex)

	reply.Accept = true
}

func (rf *Raft) CoalesceSyncLog(req *CoalesceSyncLogArgs, reply *CoalesceSyncLogReply) {
	atomic.AddInt32(&rf.SyncLogEntryCount, 1)

	rf.logUpdateLock.Lock()
	defer rf.logUpdateLock.Unlock()

	if !rf.isFollower() || req.Term < rf.term || len(req.Logs) == 0 {
		rf.logger.Printf(dLog, fmt.Sprintf("syncLog failed:%d %d %d", rf.role, req.Id, req.Term))
		return
	}

	rf.updateLastTime()

	firstLog := req.Logs[0]
	lastLog := req.Logs[len(req.Logs)-1]

	ok, preLog := rf.entry(firstLog.Index - 1)
	if ok == false || preLog.Term != req.PreTerm {
		//或者要追加的日志的前一个日志与要追加日志位置的前一个日志的term不相同：不合法
		rf.logger.Printf(dError, fmt.Sprintf("pre:%v  bug got:%v", preLog, req.PreTerm))
		return
	}

	for _, log := range req.Logs {
		if rf.logLength() == log.Index {
			rf.logs = append(rf.logs, log)
		} else {
			rf.logs[rf.logIndex(log.Index)] = log
		}
	}

	reply.Index = lastLog.Index
	if lastLog.Index < rf.logLength()-1 {
		ok2, log2 := rf.entry(lastLog.Index + 1)
		if ok2 && lastLog.Term != log2.Term {
			//去除多余的日志
			rf.logs = rf.logs[:rf.logIndex(lastLog.Index+1)]
		}
	}

	rf.logger.Printf(dLog2, fmt.Sprintf("lt [%d,%d] <-- %d receive last index=%d",
		req.Logs[0].Index, req.Logs[len(req.Logs)-1].Index, req.Id, reply.Index))
	rf.persist()
}

func (rf *Raft) AppendLog(req *RequestSyncLogArgs, reply *RequestSyncLogReply) {
	atomic.AddInt32(&rf.SyncLogEntryCount, 1)

	rf.logUpdateLock.Lock()
	defer rf.logUpdateLock.Unlock()

	reply.Accept = false

	if !rf.isFollower() || req.Term < int(rf.term) {
		rf.logger.Printf(dError, fmt.Sprintf("appendLog failed:%d %d", rf.role, req.Term))
		return
	}

	rf.updateLastTime()

	index := req.Index
	preTerm := req.PreLogTerm

	ok, log := rf.entry(index - 1)

	if !ok || log.Term != preTerm || index <= rf.commitIndex {
		//要追加的日志下标在之前的最后一个日志还要后边：不合法的
		//或者要追加的日志的前一个日志与要追加日志位置的前一个日志的term不相同：不合法
		return
	} else if rf.logLength() == index {
		rf.logs = append(rf.logs, &LogEntry{Index: index, Command: req.Command, Term: req.Term})
		reply.Accept = true
	} else {
		rf.logs[rf.logIndex(index)] = &LogEntry{Index: index, Command: req.Command, Term: req.Term}
		ok2, log2 := rf.entry(index + 1)
		if ok2 && req.Term != log2.Term {
			//去除多余的日志
			rf.logs = rf.logs[:rf.logIndex(index+1)]
		}
		reply.Accept = true
	}
	rf.logger.Printf(dLog2, fmt.Sprintf("lt index=%d resp:%v <--", req.Index, reply.Accept))
	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//不可以和提交日志同时进行
	rf.commitLogLock.Lock()
	defer rf.commitLogLock.Unlock()
	if !rf.isFollower() || args.Term < rf.term || args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.logger.Printf(dError, fmt.Sprintf("IS failed:%d %d %d", rf.role, rf.term, args.Term))
		reply.Accept = false
		return
	}

	rf.applyIndex = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.logger.Printf(dSnap, fmt.Sprintf("IS<--%d index:%d success", args.Id, args.LastIncludedIndex))
}
