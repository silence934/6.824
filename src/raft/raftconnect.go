package raft

import (
	"sync/atomic"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Id            int
	Term          int32
	CommitIndex   int32
	CommitLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Accept bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	term := args.Term

	if term < rf.term {
		reply.Accept = false
	} else {
		rf.term = term
		if rf.accept(args.CommitIndex, args.CommitLogTerm) && !(atomic.CompareAndSwapInt32(&rf.role, follower, candidate) || atomic.CompareAndSwapInt32(&rf.role, leader, candidate)) {
			rf.lastCallTime = time.Now()
			rf.role = follower
			reply.Accept = true
		} else {
			reply.Accept = false
		}
	}

	//logger.Infof("raft[%d  %d]收到投票请求对方[%d]的term:%d 结果=%v,CommitIndex:%d,我的term:%d CommitIndex:%d", rf.me, rf.role, args.Id, args.Term, reply.Accept, args.CommitIndex, rf.term, rf.commitIndex)
	logger.Infof("raft[%d]收到投票请求[%d]  结果=%v", rf.me, args.Id, reply.Accept)

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int) bool {
	commitLogTerm := -1
	if int(rf.commitIndex) != -1 {
		commitLogTerm = rf.logs[rf.commitIndex].term
	}
	args := RequestVoteArgs{Term: rf.term, CommitIndex: rf.commitIndex, CommitLogTerm: commitLogTerm, Id: rf.me}

	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	//logger.Debugf("raft[%d]收到投票回应[%d]:%v", rf.me, server, reply.Accept)
	if reply.Accept {
		v := atomic.AddInt32(&rf.vote, 1)
		if int(v) > len(rf.peers)/2 {
			if atomic.CompareAndSwapInt32(&rf.role, candidate, leader) {
				rf.peerInfos = make([]peerInfo, len(rf.peers))
				for i := 0; i < len(rf.peerInfos); i++ {
					rf.peerInfos[i] = peerInfo{index: len(rf.logs) - 1, syncLock: 0}
				}
				logger.Infof("raft[%d] 成为leader,term:%d", rf.me, rf.term)
			}
		}
	}

	return ok
}

type RequestHeartbeatArgs struct {
	// Your data here (2A, 2B).
	Term int32

	CommitIndex   int32
	LogTerm       int
	LogIndex      int
	CommitLogTerm int
}

type RequestHeartbeatReply struct {
	Accept         bool
	LogIsAlignment bool
	LogLength      int
	// Your data here (2A).
}

func (rf *Raft) Heartbeat(args *RequestHeartbeatArgs, reply *RequestHeartbeatReply) {
	// Your code here (2A, 2B). todo rf.accept(args.CommitIndex, args.CommitLogTerm)
	if rf.term <= args.Term {
		rf.lastCallTime = time.Now()
		rf.term = args.Term
		//if rf.role == leader {
		//	logger.Errorf("raft[%d]  leader ==> follower", rf.me)
		//}
		rf.role = follower

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

func (rf *Raft) sendHeartbeat(server int) bool {
	req := rf.createHeartbeatArgs(len(rf.logs) - 1)
	resp := RequestHeartbeatReply{}

	ok := rf.peers[server].Call("Raft.Heartbeat", &req, &resp)

	if ok {
		if !resp.Accept {
			//logger.Infof("raft[%d]不接受leader[%d]的心跳请求!", server, rf.me)
			return false
		} else if !resp.LogIsAlignment {
			//logger.Warnf("raft[%d]的日志与当前leader[%d]不匹配!", server, rf.me)
			rf.syncLogs(server)
		} else if rf.peerInfos[server].index >= 0 && int(rf.commitIndex) < rf.peerInfos[server].index {
			log := rf.logs[rf.peerInfos[server].index]
			if int(atomic.LoadInt32(&log.syncCount)) > (len(rf.peers) / 2) {
				logger.Infof("日志[index=%d]已经同步到大多数节点", log.index)
				rf.commitLog(log.index)
			}
		}
	}

	logger.Debugf("[heartbeat]  %d----->%d   [resp :%v]", rf.me, server, resp)

	return ok
}

func (rf *Raft) syncLogs(server int) {
	if atomic.CompareAndSwapInt32(&rf.peerInfos[server].syncLock, 0, 1) {
		defer func() { rf.peerInfos[server].syncLock = 0 }()

		index := rf.peerInfos[server].index
		for true {

			req := rf.createHeartbeatArgs(index)
			resp := RequestHeartbeatReply{}

			ok := rf.peers[server].Call("Raft.Heartbeat", &req, &resp)

			if ok {
				if resp.LogIsAlignment {
					break
				}
				if resp.LogLength > 0 {
					index = resp.LogLength
				}
			}
			index--
		}
		length := len(rf.logs)
		logger.Warnf("raft[%d]的日志与当前leader[%d log length=%d]日志最后相同的index=%d!", server, rf.me, length, index)
		for i := index + 1; i < length; i++ {
			if rf.sendSyncLog(server, rf.logs[i]) {
				atomic.AddInt32(&rf.logs[i].syncCount, 1)
			}
		}
		rf.peerInfos[server].index = length - 1

	}
}

func (rf *Raft) createHeartbeatArgs(logIndex int) RequestHeartbeatArgs {
	if logIndex < 0 {
		return RequestHeartbeatArgs{Term: rf.term, LogTerm: -1, LogIndex: -1, CommitIndex: -1, CommitLogTerm: -1}
	}
	log := rf.logs[logIndex]

	commitIndex := atomic.LoadInt32(&rf.commitIndex)
	commitLogTerm := -1
	if int(commitIndex) != -1 {
		commitLogTerm = rf.logs[commitIndex].term
	}
	return RequestHeartbeatArgs{
		Term:          rf.term,
		LogTerm:       log.term,
		LogIndex:      log.index,
		CommitIndex:   commitIndex,
		CommitLogTerm: commitLogTerm,
	}
}

func (rf *Raft) accept(commitIndex int32, term int) bool {
	//return rf.term < term || rf.commitIndex <= commitIndex
	if rf.commitIndex < commitIndex {
		return true
	} else if rf.commitIndex > commitIndex {
		return false
	} else {
		return commitIndex == -1 || rf.logs[int(commitIndex)].term <= term
	}
}

//提交的条件：
//1.日志同步次数超过raft数量的一半
//2.这个日志是最后一条日志
//3.这个日志的term是当前term
func (rf *Raft) commitLog(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if int(rf.commitIndex) == index || index != len(rf.logs)-1 {
		return
	}
	rf.commitIndex = int32(index)
	logger.Infof("leader[%d]提交了日志,当前commitIndex=%d", rf.me, index)
}

type RequestSyncLogArgs struct {
	// Your data here (2A, 2B).
	Index   int
	Term    int
	Command interface{}
}

type RequestSyncLogReply struct {
	// Your data here (2A).
}

func (rf *Raft) SyncLog(args *RequestSyncLogArgs, reply *RequestSyncLogReply) {
	rf.lastCallTime = time.Now()
	rf.syncLogLock.Lock()
	defer rf.syncLogLock.Unlock()

	if len(rf.logs) == args.Index {
		//todo 扩容
		rf.logs = append(rf.logs, LogEntry{index: args.Index, command: args.Command, term: args.Term})
	} else {
		rf.logs[args.Index] = LogEntry{index: args.Index, command: args.Command, term: args.Term}
	}
}

func (rf *Raft) sendSyncLog(server int, entry LogEntry) bool {
	args := RequestSyncLogArgs{Index: entry.index, Term: entry.term, Command: entry.command}
	reply := RequestSyncLogReply{}
	ok := rf.peers[server].Call("Raft.SyncLog", &args, &reply)
	logger.Infof("leader[%d]向raft[%d]节点发送日志,index=%d --->%v", rf.me, server, entry.index, ok)
	return ok
}
