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
	Index   int
	Term    int
	Command interface{}
}

func (t *LogEntry) String() string {
	return fmt.Sprintf("{%d %d}", t.Index, t.Term)
}

type peerInfo struct {
	serverId        int
	index           int //对方和自己相同的日志下标
	expIndex        int32
	updateIndexLock *sync.RWMutex
	//channel         chan RequestSyncLogArgs //日志同步缓存channel
	commitChannel   chan *CommitLogArgs //日志提交缓存
	heartbeatTicker *time.Ticker
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu            *sync.Mutex
	initPeers     int32
	voteLock      *sync.Mutex
	commitLogLock *sync.Mutex
	persistLock   *sync.Mutex
	logUpdateLock *sync.RWMutex
	heartbeat     *time.Ticker

	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	dead              int32               // set by Kill()
	logger            Log
	heartbeatInterval time.Duration

	me         int // this peer's Index into peers[]
	applyIndex int //刷入applyCh的下标
	role       int32

	applyCh      chan ApplyMsg
	peerInfos    []*peerInfo
	lastCallTime time.Time
	vote         int32 //得票数
	term         int32
	logs         []*LogEntry
	commitIndex  int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	lastIncludedTerm  int
	lastIncludedIndex int
	snapshot          []byte

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
	//不知道为什么并发下似乎没有持久化成功
	rf.persistLock.Lock()
	defer rf.persistLock.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.logs)
	//e.Encode(rf.applyIndex)
	data := w.Bytes()
	//rf.persister.SaveRaftState(data)
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	rf.logger.Printf(dPersist, fmt.Sprintf("persist,logs length:%d", len(rf.logs)))
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
		if rf.persister.SnapshotSize() > 0 {
			rf.snapshot = rf.persister.ReadSnapshot()
		}
		if len(logs) > 0 {
			rf.term = term
			rf.logs = logs
			rf.logger.Printf(dPersist, fmt.Sprintf("readPersist,logs length:%d", len(logs)))
			log := logs[0]
			//重启过后要重新提交日志
			rf.applyIndex = log.Index
			rf.lastIncludedIndex = log.Index
			rf.lastIncludedTerm = log.Term
		}
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.logUpdateLock.Lock()
	defer rf.logUpdateLock.Unlock()

	if lastIncludedIndex <= rf.lastIncludedIndex {
		return false
	}

	rf.logger.Printf(dSnap, fmt.Sprintf("CondInstallSnapshot %d", lastIncludedIndex))

	rf.snapshot = snapshot
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.logs = []*LogEntry{{Term: lastIncludedTerm, Index: lastIncludedIndex}}
	rf.persist()
	return true
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the Log through (and including)
// that Index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.logUpdateLock.Lock()
	defer rf.logUpdateLock.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	//rf.logger.Printf(dSnap, fmt.Sprintf("Snapshot %d", index))

	rf.snapshot = snapshot
	//commitIndex不需要修改
	rf.lastIncludedTerm = rf.logs[rf.logIndex(index)].Term
	if rf.logIndex(index)+1 == len(rf.logs) {
		rf.logs = []*LogEntry{{Term: rf.lastIncludedTerm, Index: index}}
	} else {
		rf.logs = append([]*LogEntry{{Term: rf.lastIncludedTerm, Index: index}}, rf.logs[rf.logIndex(index+1):]...)
	}
	rf.logger.Printf(dSnap, fmt.Sprintf("Snapshot index:%d,log length:%d", index, len(rf.logs)))
	rf.lastIncludedIndex = index
	rf.persist()
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	//term 和isLeader不可交换顺序
	term := int(rf.term)
	isLeader := rf.isLeader()

	if isLeader {
		//complete := make([]bool, len(rf.peers))
		//complete[rf.me] = true
		entry := LogEntry{Term: term, Command: command}
		index = rf.addLogEntry(&entry)
		rf.logger.Printf(dClient, fmt.Sprintf("al [Index:%d,value:%v]", index, commandToString(command)))
	}

	return index, term, isLeader
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
	rf.stopHeartbeatLoop()
	rf.logger.Printf(dDrop, "raft node killed")
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
		ms := 165 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		//logger.Debugf("raft[%d-%d] ms call time:%v", rf.me, rf.role, rf.lastCallTime)
		if !rf.isLeader() {
			//rf.logger.Printf(dLog, fmt.Sprintf("ticker timeout  %v  ", time.Now().Sub(rf.lastTime())))

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

	for _, data := range rf.peerInfos {
		if data.serverId != rf.me {
			go func(info *peerInfo) {
				for range info.heartbeatTicker.C {
					go func(server int) {
						rf.sendHeartbeat(server)
					}(info.serverId)
				}
			}(data)
		}
	}

	//for range rf.heartbeat.C {
	//	//time.Sleep(150 * time.Millisecond)
	//	//time.Tick()
	//	if rf.isLeader() {
	//		for i := range rf.peers {
	//			if i != rf.me {
	//				go rf.sendHeartbeat(i, rf.logLength()-1)
	//			}
	//		}
	//	}
	//}
}

func (rf *Raft) logBufferLoop() {

	for !rf.killed() {
		if rf.isLeader() {
			for _, peer := range rf.peerInfos {

				go func(peer *peerInfo) {
					server := peer.serverId

					args := CommitLogArgs{Id: rf.me, Term: rf.term, CommitIndex: -1, CommitLogTerm: -1}

					end := false
					for true {
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
						go func(serverId int, args *CommitLogArgs) {
							rf.logger.Printf(dCommit, fmt.Sprintf("send commit -->%d index:%d", serverId, args.CommitIndex))
							ok := rf.call(serverId, "Raft.CommitLog", args, &CommitLogReply{})
							if !ok {
								rf.logger.Printf(dTimer, fmt.Sprintf("send commit -->%d index:%d fail", serverId, args.CommitIndex))
							}
						}(server, &args)
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
	rf.logUpdateLock = &sync.RWMutex{}
	rf.voteLock = &sync.Mutex{}
	rf.persistLock = &sync.Mutex{}
	rf.commitLogLock = &sync.Mutex{}
	rf.mu = &sync.Mutex{}
	rf.heartbeatInterval = 150 * time.Millisecond

	rf.applyCh = applyCh
	rf.role = follower
	rf.commitIndex = 0
	rf.logs = []*LogEntry{{Term: -1, Index: 0}}

	rf.applyIndex = 0
	rf.lastIncludedTerm = -1
	rf.lastIncludedIndex = 0
	rf.logger = MakeLogger(rf)
	rf.logger.Printf(dDrop, "raft node start")

	rf.peerInfos = make([]*peerInfo, len(rf.peers))
	for i := 0; i < len(rf.peerInfos); i++ {
		rf.peerInfos[i] = &peerInfo{
			serverId:        i,
			updateIndexLock: &sync.RWMutex{},
			//channel:         make(chan RequestSyncLogArgs, 20),
			commitChannel:   make(chan *CommitLogArgs, 20),
			heartbeatTicker: time.NewTicker(rf.heartbeatInterval),
		}
	}
	rf.stopHeartbeatLoop()

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.heartbeatLoop()
	go rf.logBufferLoop()

	return rf
}
