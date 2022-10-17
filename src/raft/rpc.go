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
			i := reqIndex - 1
			for true {
				o, l := rf.entry(i)
				if o && l.Term == log.Term {
					i--
				} else {
					break
				}
			}
			reply.FirstIndex = i + 1
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
				i := log.Index - 1
				for true {
					o, l := rf.entry(i)
					if o && l.Term == log.Term {
						i--
					} else {
						break
					}
				}
				reply.FirstIndex = i + 1
			}
		}

	} else {
		reply.Accept = false
		rf.logger.Printf(dLog, fmt.Sprintf("hb <-- id:%d term:%d false", args.Id, args.Term))
	}
	//logger.Debugf("[heartbeat] [%d %d] -----> [%d %d]  resp:%v", args.Id, args.Term, rf.me, term, reply.Accept)

}

func (rf *Raft) CommitLog(args *CommitLogArgs, reply *CommitLogReply) {
	atomic.AddInt32(&rf.CommitLogCount, 1)

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

	rf.logger.Printf(dCommit, fmt.Sprintf("commit log=%+v applyIndex:%d", log, rf.applyIndex))

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
	rf.snapshotLock.Lock()
	defer func() {
		rf.snapshotLock.Unlock()
		rf.unlockSyncLog()
		rf.logger.Printf(dLog2, fmt.Sprintf("lt startIndex=%d length=%d <--%d   receive=%d",
			req.Logs[0].Index, len(req.Logs), req.Id, len(reply.Indexes)))
	}()

	rf.updateLastTime()

	var lastLog LogEntry
	for _, args := range req.Logs {
		index := args.Index
		preTerm := args.PreLogTerm

		ok, preLog := rf.entry(index - 1)
		if !ok || preLog.Term != preTerm {
			//要追加的日志不在接受日志范围内(lastIndex,logLength-1)
			//或者要追加的日志的前一个日志与要追加日志位置的前一个日志的term不相同：不合法
			rf.logger.Printf(dError, fmt.Sprintf("pre:%v  bug got:%v", preLog, args))
			return
		} else {
			entry := LogEntry{Index: index, Command: args.Command, Term: args.Term}
			lastLog = entry
			if rf.logLength() == index {
				rf.logs = append(rf.logs, entry)
			} else {
				rf.logs[rf.logIndex(index)] = entry
			}
		}
		reply.Indexes = append(reply.Indexes, &index)
	}

	ok2, log2 := rf.entry(lastLog.Index + 1)
	if ok2 && lastLog.Term > log2.Term {
		//去除多余的日志

		rf.logs = rf.logs[:rf.logIndex(lastLog.Index+1)]
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
	rf.updateLastTime()
	defer func() {
		rf.logger.Printf(dLog2, fmt.Sprintf("lt startIndex=%d resp:%v <--", req.Index, reply.Accept))
	}()

	index := req.Index
	preTerm := req.PreLogTerm

	ok, log := rf.entry(index - 1)

	if !ok || log.Term != preTerm || index <= rf.commitIndex {
		//要追加的日志下标在之前的最后一个日志还要后边：不合法的
		//或者要追加的日志的前一个日志与要追加日志位置的前一个日志的term不相同：不合法
		return
	} else if rf.logLength() == index {
		rf.logs = append(rf.logs, LogEntry{Index: index, Command: req.Command, Term: req.Term})
		reply.Accept = true
	} else {
		rf.snapshotLock.Lock()
		defer rf.snapshotLock.Unlock()
		rf.logs[rf.logIndex(index)] = LogEntry{Index: index, Command: req.Command, Term: req.Term}
		ok2, log2 := rf.entry(index + 1)
		if ok2 && req.Term > log2.Term {
			//去除多余的日志
			rf.logs = rf.logs[:rf.logIndex(index+1)]
		}
		reply.Accept = true
	}

	rf.persist()
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
