package raft

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

type RequestSyncLogArgs struct {
	// Your data here (2A, 2B).
	PreLogTerm int
	Index      int
	Term       int
	Command    interface{}
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
