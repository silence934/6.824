package raft

import "fmt"

type RequestHeartbeatArgs struct {
	// Your data here (2A, 2B).
	Id    int
	Term  int32
	Index int
}

type RequestHeartbeatReply struct {
	Accept bool

	FirstIndex int
	LogIndex   int
	LogTerm    int
}

type RequestSyncLogArgs struct {
	// Your data here (2A, 2B).
	PreLogTerm int
	Index      int
	Term       int
	Command    interface{}
}

func (t *RequestSyncLogArgs) String() string {
	return fmt.Sprintf("{Index:%d}", t.Index)
}

type RequestSyncLogReply struct {
	Accept bool
	// Your data here (2A).
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Id          int
	Term        int32
	LogsLength  int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Accept bool
	Term   int32
}

type CommitLogArgs struct {
	// Your data here (2A, 2B).Ω
	Id            int
	Term          int32
	CommitIndex   int32
	CommitLogTerm int
}

type CommitLogReply struct {
	// Your data here (2A).
	Accept bool
}

type CoalesceSyncLogArgs struct {
	Id   int
	Term int32
	Args []*RequestSyncLogArgs
}

type CoalesceSyncLogReply struct {
	Indexes []*int
}
