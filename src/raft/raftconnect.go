package raft

import (
	"sync/atomic"
)

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
			if rf.setRole(candidate, leader) {
				rf.peerInfos = make([]*peerInfo, len(rf.peers))
				for i := 0; i < len(rf.peerInfos); i++ {
					rf.peerInfos[i] = &peerInfo{index: len(rf.logs) - 1, syncLock: 0}
				}
				logger.Infof("raft[%d] 成为leader,term:%d", rf.me, rf.term)
			}
		}
	}

	return ok
}

func (rf *Raft) sendHeartbeat(server int) bool {
	//todo role和term获取尽量是原子获取，否则可能会出现这种情况:
	//旧leader在判断为isLeader=true之后，接收到heartbeat消息，修改term
	//导致旧leader发送的heartbeat消息中的term是现在新的term flower认为这个leader是合法的

	if !rf.isLeader() {
		return false
	}
	term := rf.term
	req := rf.createHeartbeatArgs(len(rf.logs)-1, term)
	resp := RequestHeartbeatReply{}

	ok := rf.peers[server].Call("Raft.Heartbeat", &req, &resp)

	//peerIndex := rf.getPeerIndex(server)
	if ok {
		if !resp.Accept {
			//logger.Infof("raft[%d]不接受leader[%d]的心跳请求!", server, rf.me)
			return false
		} else if !resp.LogIsAlignment {
			//logger.Warnf("raft[%d]的日志与当前leader[%d]不匹配!", server, rf.me)
			go rf.checkLogs(server, term)
		}
		//else if peerIndex >= 0 && int(rf.commitIndex) < peerIndex {
		//	//peerIndex 大于当前的 commitIndex
		//	log := rf.logs[peerIndex]
		//	if int(atomic.LoadInt32(&log.syncCount)) > (len(rf.peers) / 2) {
		//		//logger.Infof("日志[index=%d]已经同步到大多数节点", log.index)
		//		go rf.sendCommitLog(log.index, -1)
		//	}
		//}
	}

	logger.Debugf("[heartbeat]  %d----->%d   [resp :%v]", rf.me, server, resp)

	return ok
}

func (rf *Raft) sendCommitLog(commitIndex, server int) {

	args := CommitLogArgs{Id: rf.me, Term: rf.term, CommitIndex: int32(commitIndex), CommitLogTerm: rf.logs[commitIndex].term}

	if server == -1 {
		for i := range rf.peers {
			reply := CommitLogReply{}
			if i == rf.me {
				go rf.CommitLog(&args, &reply)
			} else {
				atomic.AddInt32(&rf.CommitLogCount, 1)
				go rf.peers[i].Call("Raft.CommitLog", &args, &reply)
			}
		}
	} else {
		reply := CommitLogReply{}
		if server == rf.me {
			go rf.CommitLog(&args, &reply)
		} else {
			atomic.AddInt32(&rf.CommitLogCount, 1)
			go rf.peers[server].Call("Raft.CommitLog", &args, &reply)
		}
	}
}

func (rf *Raft) checkLogs(server int, term int32) {
	if atomic.CompareAndSwapInt32(&rf.peerInfos[server].syncLock, 0, 1) {
		defer func() { rf.peerInfos[server].syncLock = 0 }()

		index := rf.getPeerIndex(server)
		for true {

			req := rf.createHeartbeatArgs(index, term)
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
		rf.setPeerIndex(server, index)
		for i := index + 1; i < length; i++ {
			if rf.sendLogEntry(server, rf.logs[i]) {
				rf.setPeerIndex(server, i)
			}
		}
	}
}

func (rf *Raft) createHeartbeatArgs(logIndex int, term int32) RequestHeartbeatArgs {
	if logIndex < 0 {
		return RequestHeartbeatArgs{Term: term, LogTerm: -1, LogIndex: -1, CommitIndex: -1, CommitLogTerm: -1}
	}
	log := rf.logs[logIndex]

	commitIndex := int(atomic.LoadInt32(&rf.commitIndex))
	commitLogTerm := -1
	if commitIndex != -1 {
		commitLogTerm = rf.logs[commitIndex].term
	}
	return RequestHeartbeatArgs{
		Term:          rf.term,
		LogTerm:       log.term,
		LogIndex:      log.index,
		CommitIndex:   int32(commitIndex),
		CommitLogTerm: commitLogTerm,
	}
}

func (rf *Raft) sendLogEntry(server int, entry *LogEntry) bool {
	expIndex := rf.getPeerIndex(server) + 1
	index := entry.index

	if expIndex < index {
		return false
	}

	args := RequestSyncLogArgs{Index: index, Term: entry.term, Command: entry.command}
	if index != 0 {
		args.PreLogTerm = rf.logs[index-1].term
	}
	reply := RequestSyncLogReply{}
	ok := rf.peers[server].Call("Raft.SyncLogEntry", &args, &reply)
	logger.Infof("leader[%d]向raft[%d]节点发送日志,index=%d --->%v", rf.me, server, entry.index, reply.Accept)

	if ok && reply.Accept {
		go rf.sendLogSuccess(index, server)
		return true
	}

	return false
}

func (rf *Raft) sendLogSuccess(index, server int) {
	log := rf.logs[index]
	count := int(atomic.AddInt32(&rf.logs[index].syncCount, 1))
	rf.setPeerIndex(server, index)

	mid := len(rf.peers)/2 + 1

	if count == mid {
		//第一次同步到过半节点  向所有节点发送提交
		go rf.sendCommitLog(log.index, -1)
	} else if count > mid {
		//logger.Infof("日志[index=%d]已经同步到大多数节点", log.index)
		//后续的可以只发送给这个节点
		go rf.sendCommitLog(log.index, server)
	}
}
