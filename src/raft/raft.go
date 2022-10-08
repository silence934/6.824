package raft

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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
	Index     int
	Term      int
	Command   interface{}
	SyncCount int32
	//Complete  []bool
}

func (t *LogEntry) String() string {
	return fmt.Sprintf("{%d %v}", t.Index, t.Command)
}

type peerInfo struct {
	serverId        int
	index           int   //对方和自己相同的日志下标
	checkLogsLock   int32 //0 未进行，1正在进行
	updateIndexLock sync.Mutex
	channel         chan RequestSyncLogArgs //日志同步缓存channel
	commitChannel   chan CommitLogArgs      //日志提交缓存
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu            sync.Mutex
	initPeers     int32
	syncLogLock   int32
	voteLock      sync.Mutex
	appendLogLock sync.Mutex
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	dead          int32               // set by Kill()
	logger        Log

	me         int // this peer's Index into peers[]
	applyIndex int //刷入applyCh的下标
	role       int32

	applyCh      chan ApplyMsg
	peerInfos    []*peerInfo
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int32
	var logs []*LogEntry
	if d.Decode(&term) != nil || d.Decode(&logs) != nil {
		rf.logger.Errorf("decode error")
	} else {
		rf.term = term
		rf.logs = logs
	}
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
// all info up to and including Index. this means the
// service no longer needs the Log through (and including)
// that Index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	//term 和isLeader不可交换顺序
	term := int(rf.term)
	isLeader := rf.isLeader()

	if isLeader {
		//complete := make([]bool, len(rf.peers))
		//complete[rf.me] = true
		entry := LogEntry{Term: term, Command: command, SyncCount: 1}
		index = rf.addLogEntry(&entry)
		rf.logger.Printf(dClient, fmt.Sprintf("al [Index:%d,value:%v]", index, commandToString(command)))
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
		ms := 450 + (rand.Int63() % 150)
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
			rf.setTerm(rf.term + 1)

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

func (rf *Raft) logBufferLoop() {

	for !rf.killed() {
		if rf.isLeader() {
			for _, peer := range rf.peerInfos {

				go func(peer *peerInfo) {
					server := peer.serverId

					args := CommitLogArgs{Id: rf.me, Term: rf.term, CommitIndex: -1, CommitLogTerm: -1}

					for true {
						end := false
						select {
						case commit, ok := <-peer.commitChannel:
							if ok {
								if commit.CommitIndex > args.CommitIndex {
									args.CommitIndex = commit.CommitIndex
									args.CommitLogTerm = commit.CommitLogTerm
								}
							} else {
								end = true
							}
						default:
							end = true
						}
						if end {
							break
						}
					}

					if args.CommitIndex != -1 {
						go rf.peers[server].Call("Raft.CommitLog", &args, &CommitLogReply{})
					}
				}(peer)
			}
		}
		time.Sleep(21 * time.Millisecond)
	}

}

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

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatLoop()
	go rf.logBufferLoop()

	//PrettyDebug(dTimer, "S%d, apply log, log index=%v, log term=%v, log command=%v", rf.me, 1, 2, 3)
	//PrettyDebug(dInfo, "S%d, apply log, log index=%v, log term=%v, log command=%v", rf.me, 1, 2, 3)
	//PrettyDebug(dTrace, "S%d, apply log, log index=%v, log term=%v, log command=%v", rf.me, 1, 2, 3)

	//logger.Infof("S%d, apply log, log index=%v, log term=%v, log command=%v", rf.me, 1, 2, 3)

	rf.logger = MakeLog(rf)

	return rf
}
