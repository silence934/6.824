package raft

import (
	"fmt"
)

type RequestHeartbeatArgs struct {
	// Your data here (2A, 2B).
	Id    int
	Term  int32
	Index int
}

type RequestHeartbeatReply struct {
	Accept bool

	FirstIndex  int
	LogIndex    int
	LogTerm     int
	CommitIndex int
	RespTime    int64
}

func (r *RequestHeartbeatReply) String() string {
	return fmt.Sprintf("{%d %d %d %d}", r.FirstIndex, r.LogIndex, r.LogTerm, r.CommitIndex)
}

type RequestSyncLogArgs struct {
	// Your data here (2A, 2B).
	PreLogTerm int
	Index      int
	Term       int
	Command    interface{}
}

func (t *RequestSyncLogArgs) String() string {
	//return fmt.Sprintf("{Index:%d}", t.Index)
	return fmt.Sprintf("%+v", *t)
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
	Id           int
	Term         int32
	LastLogIndex int
	LastLogTerm  int
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
	CommitIndex   int
	CommitLogTerm int
}

type CommitLogReply struct {
	// Your data here (2A).
	Accept bool
}

type CoalesceSyncLogArgs struct {
	Id      int
	Term    int32
	PreTerm int
	Logs    []*LogEntry
}

type CoalesceSyncLogReply struct {
	Index int
}

type InstallSnapshotArgs struct {
	Id                int
	Term              int32
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Accept bool
}
