package raft

//一些结构
type State int
const (
	Follower State = iota
	Candidate
	Leader
)

//Log structure
type LogEntry struct {
	Index int
	Term  int
	Command interface{}
}

// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}


//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term     int
	CandidateId  int
	LastLogIndex  int
	LastLogTerm   int
}
type RequestVoteReply struct {
	// Your data here (2A).
	Term     int
	VoteGranted  bool
}

//
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term     int
	LeaderId  int
	PreLogIndex  int
	PreLogTerm   int
	Entries     []LogEntry
	LeaderCommit  int
}
type AppendEntriesReply struct {
	Term     int
	Success  bool

	CIndex   int
	CTerm    int
	ClastLogIndex int
}

type InstallSnapshotArgs struct {
	Term   int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset   int
	Data      []byte
	Done      bool
}

type InstallSnapshotReply struct {
	 Term  int
}