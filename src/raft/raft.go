package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

var logger = log.Logger()

const (
	follower = iota
	candidate
	leader
)

//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	index     int
	term      int
	command   interface{}
	syncCount int32
}

type peerInfo struct {
	serverId        int
	index           int   //对方和自己相同的日志下标
	checkLogsLock   int32 //0 未进行，1正在进行
	updateIndexLock sync.Mutex
	channel         chan RequestSyncLogArgs //日志同步缓存channel
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	syncLogLock int32
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()

	applyIndex   int
	applyCh      chan ApplyMsg
	peerInfos    []*peerInfo
	role         int32
	lastCallTime time.Time
	vote         int32 //得票数
	term         int32
	logs         []*LogEntry
	commitIndex  int32
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	RequestVoteCount  int32
	HeartbeatCount    int32
	CommitLogCount    int32
	SyncLogEntryCount int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.term), rf.isLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := int(rf.term)
	isLeader := rf.isLeader()

	if isLeader {
		entry := LogEntry{term: term, command: command, syncCount: 1}
		index = rf.appendLog(&entry)

		logger.Infof("leader[%d]收到日志[index:%d,value:%v]添加请求", rf.me, index, commandToString(command))
		for i := range rf.peers {
			if i != rf.me {
				rf.sendLogEntry(i, &entry)
			}
		}

	}

	return index + 1, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each sync,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		ms := 150 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		//logger.Debugf("raft[%d-%d] ms call time:%v", rf.me, rf.role, rf.lastCallTime)
		if !rf.isLeader() {
			if !time.Now().After(rf.lastTime().Add(time.Duration(ms) * time.Millisecond)) {
				continue
			}

			if !(rf.setRole(follower, candidate) || rf.setRole(candidate, candidate)) {
				continue
			}

			rf.vote = 1
			rf.term++

			for i := range rf.peers {
				if i != rf.me {
					go rf.sendRequestVote(i)
				}
			}

		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

func (rf *Raft) heartbeatLoop() {
	for rf.killed() == false {
		time.Sleep(150 * time.Millisecond)
		if rf.isLeader() {
			for i := range rf.peers {
				if i != rf.me {
					go rf.sendHeartbeat(i)
				}
			}
		}
	}
}

func (rf *Raft) messageLoop() {
	for rf.killed() == false {
		time.Sleep(300 * time.Millisecond)
		rf.mu.Lock()
		for i := rf.applyIndex + 1; i <= int(atomic.LoadInt32(&rf.commitIndex)); i++ {
			item := rf.logs[i]
			//if item.command == nil {
			//	logger.Errorf("raft[%d]出现意想不到的情况:%+v   %v", rf.me, item, rf.logs)
			//}
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: item.command, CommandIndex: item.index + 1}
			logger.Debugf("raft[%d]向applyCh输入数据 CommandIndex=%d", rf.me, item.index+1)
			rf.applyIndex++
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) flushLog(commitIndex int) {
	for i := rf.applyIndex + 1; i <= commitIndex; i++ {
		item := rf.logs[i]
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: item.command, CommandIndex: item.index + 1}
		logger.Debugf("raft[%d]向applyCh输入数据 CommandIndex=%d", rf.me, item.index+1)
		rf.applyIndex++
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyIndex = -1
	rf.applyCh = applyCh
	rf.role = follower
	rf.commitIndex = -1
	rf.logs = []*LogEntry{}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatLoop()
	go rf.logEntryLoop()
	//go rf.messageLoop()

	return rf
}
