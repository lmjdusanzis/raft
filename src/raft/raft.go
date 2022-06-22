package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
	"../labrpc"
	"../labgob"
)
import "sync/atomic"


// import "bytes"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state        State
	currentTerm  int
	votedFor     int
	log          []LogEntry

	commitIndex  int
	lastApplied  int


	nextIndex    []int
	matchIndex   []int

	lastIncludedIndex  int
	lastIncludedTerm   int

	preTime      time.Time
	electionTimer	time.Duration
	heartBeatTimer   time.Duration
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.serializeState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	err = e.Encode(rf.votedFor)
	err = e.Encode(rf.log)
	err = e.Encode(rf.lastIncludedIndex)
	err = e.Encode(rf.lastIncludedTerm)

	if err != nil {
		panic("persist failed")
	}
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("readPersist failed")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.mu.Unlock()
	}
}

func (rf *Raft) TakeSnapshot(lastIncludedIndex int, lastIncludedTerm int, rawSnapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied > lastIncludedIndex ||                        //如果需要做快照的点，小于已经被应用的点，快照做不出来
		rf.lastIncludedIndex >= lastIncludedIndex {					//如果小于等于现有的快照点，做了没意义
		return
	}

	log := make([]LogEntry, 0)											// 做完快照后，log要重做
	log = append(log, rf.log[rf.getIndexAtLog(lastIncludedIndex):]...)

	rf.log = log													//新做的日志
	rf.lastIncludedIndex = lastIncludedIndex						//新的日志点
	rf.lastIncludedTerm = lastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.serializeState(), rawSnapshot)
}


func (rf *Raft) getIndexAtLog(index int) int {
	if index < rf.lastIncludedIndex {
		panic("use logIndex already in snapshot")
	}
	return index - rf.lastIncludedIndex
}

func (rf *Raft) getIndexOfLastLog() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log) - 1]
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {						//对于任期比我们小的，一律拒绝。并告知发送方我们已知的大任期
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {				//对于任期比我们大的，先改变自身状态
		rf.currentTerm = args.Term
		rf.persist()									//第四次持久化
		rf.state = Follower

		if rf.electRestrition(args) {					//对于所有任期大于我们的报文进行，选举约束判断
			rf.agreeToVote(args, reply)					//投票要求很简单，你的最新日志上的任期号比我大
			return										//或者咱俩任期一样，但是你的日志至少要长于等于我的日志
		}
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.electRestrition(args) {					// 对于任期等于我们任期的朋友，只有当我第一次投票。或者上次也投你
			rf.agreeToVote(args, reply)					//你才有机会进行投票约束判断
			return
		}
	}
	reply.VoteGranted = false							//对于没有通过投票约束的，只能再见了
	reply.Term = rf.currentTerm
}


func (rf *Raft) electRestrition (args *RequestVoteArgs) bool {
	if rf.getLastLog().Term < args.LastLogTerm ||
		(rf.getLastLog().Term == args.LastLogTerm && rf.getLastLog().Index <= args.LastLogIndex) {
		return true
	}
	return  false
}

func (rf *Raft) agreeToVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("Follower  %d  term %d  " + "agree to vote for %d",
		rf.me, rf.currentTerm, args.CandidateId)
	rf.preTime = time.Now()								//当我们同意为发送方投票，自然votedFor改为发送方，触发持久化
	rf.state = Follower									//并且我们将成为Follower，得重置选举时间
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.persist()    									//第五次持久化

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed()  || rf.state != Leader{
		return -1, -1, false
	}

	index = rf.getLastLog().Index + 1
	term = rf.currentTerm
	isLeader = rf.state == Leader

	rf.log = append(rf.log, LogEntry{
		Term: term,
		Index: index,
		Command: command,
	})

	rf.persist() // 第一次持久化

	DPrintf("Leader    %d term %d  " + "get command %v index %d from client ",
		rf.me, rf.currentTerm, command, index)

	return index, term, isLeader
}

func (rf *Raft) run() {
	for  {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		switch rf.state {
		case Follower:
			rf.mu.Unlock()
			rf.runFollower()
		case Candidate:
			rf.mu.Unlock()
			rf.runCandidate()
		case Leader:
			rf.mu.Unlock()
			rf.initLeader()
			rf.runLeader()
		}
	}
}

func (rf *Raft) runFollower() {
	for {
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		preTime := rf.preTime				//这里我们不直接用rf.electionTimer是因为线程在Sleep时会持有锁
		electionTimer := rf.electionTimer

		if time.Now().Sub(preTime) > electionTimer {
			DPrintf("Follower  %d  term %d  " + "election timeout", rf.me, rf.currentTerm)
			rf.state = Candidate
			rf.mu.Unlock()
			break
		} else {
			rf.mu.Unlock()				//所以在Sleep之前需要释放锁，用局部变量代替Raft自身的字段
			time.Sleep(electionTimer - time.Now().Sub(preTime))
		}
	}
}

func (rf *Raft) runCandidate() {
	for {
		if rf.killed() {
			break
		}
		rf.mu.Lock()

		if rf.state != Candidate {		//可以是收到半数投票后转为Leader
			rf.mu.Unlock()				//也可是收到Leader的心跳，追加项
			break						//或者更大任期的Candidate来了
		}

		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist() 					// 第二次持久化

		rf.electionTimer = time.Duration(200 + rand.Intn(200)) * time.Millisecond
		rf.preTime = time.Now()
		DPrintf("Candidate %d  term %d  " +
			"initiate a new election", rf.me, rf.currentTerm)
		rf.mu.Unlock()						// 发送报文之前一定要释放锁

		agree, sent := 0, 0
		cond := sync.NewCond(&rf.mu)

		awakenAll(&rf.mu, cond, rf.electionTimer)				//这里防止所有的发送报文的协程都挂了，而没人唤醒主线程
		rf.sendRequestVoteToAll(&agree, &sent, cond);

		rf.mu.Lock()
		for agree + 1 <= len(rf.peers) && sent < len(rf.peers) - 1 &&
			time.Now().Sub(rf.preTime) < rf.electionTimer && rf.state == Candidate {
			cond.Wait()					// 主线程等待，等赞同数大于一半，等接受消息接受了全部，等计时器超时，等Candidate状态改变
		}

		if rf.state != Candidate {
			DPrintf("Candidate %d term %d  " + "state changed",
				rf.me, rf.currentTerm)
			rf.mu.Unlock()
			break
		}

		if agree + 1 > len(rf.peers) / 2 {
			DPrintf("Candidate %d term %d  " + "become new Leader",
				rf.me, rf.currentTerm)
			rf.state = Leader
			rf.mu.Unlock()
			break
		}

		DPrintf("Candidate %d term %d  " + "fail to get enough votes on time", rf.me, rf.currentTerm)

		preTime := rf.preTime
		electionTimer := rf.electionTimer
		rf.mu.Unlock()
		if  time.Now().Sub(preTime) < electionTimer{
			time.Sleep(electionTimer - time.Now().Sub(preTime))
		}
	}
}

func awakenAll(mu *sync.Mutex, cond *sync.Cond, duration time.Duration)  {
	go func() {
		time.Sleep(duration)
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}()
}
func (rf *Raft) sendRequestVoteToAll(agree *int, sent *int, cond *sync.Cond)  {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peerId int) {
			rf.mu.Lock()
			if rf.state != Candidate {				// 发送前检查自身的状态
				DPrintf("Candidate %d term %d  " + "state changed while voting",
					rf.me, rf.currentTerm)
				*sent++
				cond.Broadcast()
				rf.mu.Unlock()
				return
			}

			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLog().Index,
				LastLogTerm:  rf.log[rf.getIndexOfLastLog()].Term,
			}
			reply := RequestVoteReply{}
			DPrintf("Candidate %d term %d  " + "request vote from %d",
				rf.me, rf.currentTerm, peerId)
			rf.mu.Unlock()
			//发送前解锁

			ok := rf.sendRequestVote(peerId, &args, &reply)

			//发送后加锁
			rf.mu.Lock()
			if !ok {
				*sent++
				cond.Broadcast()
				rf.mu.Unlock()
				return
			}

			if rf.state != Candidate || args.Term != rf.currentTerm {
				DPrintf("Candidate %d term %d  " + "state changed or reply outdated",
					rf.me, rf.currentTerm)
				*sent++							//当已成为Leader，我们不需要统计后续的vote reply
				cond.Broadcast()				// 或被成为Follower，我们也不再具备统计vote reply的权利
				rf.mu.Unlock()
				return
			}

			*sent++
			if reply.VoteGranted {
				*agree++									//赞成票，我们统计下
			} else if reply.Term > rf.currentTerm {
				DPrintf("Candidate %d term %d  " + "find larger Term from %d",
					rf.me, rf.currentTerm, peerId)			//反对票，可能接受方之前已经投过了那些
				rf.currentTerm = reply.Term					//任期小于等于我们的Candidate，我们不予理睬
				rf.votedFor = -1							//也可能投给了任期比我们大的，我们要改变状态
				rf.persist() 								// 第三次持久化
				rf.state = Follower
			}
			cond.Broadcast()
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	length := len(rf.peers)									//成为leader后的第一件事就是重新与follower进行同步
	for i := 0; i < length; i++ {
		rf.nextIndex[i] = rf.getLastLog().Index + 1		//默认需要同步到leader最新的日志
		rf.matchIndex[i] = 0								//默认follower上已同步的只有最初的哨兵项
	}
}

func (rf *Raft) runLeader() {
	for {
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		if rf.state != Leader {								//照例检查自己是否是Leader
			DPrintf("Leader    %d term %d  " + "state changed",
				rf.me, rf.currentTerm)
			rf.mu.Unlock()
			break
		}

		rf.preTime = time.Now()
		DPrintf("Leader    %d term %d  " + "prepare AE to all",
			rf.me, rf.currentTerm)
		rf.mu.Unlock()

		agree, sent := 0, 0
		cond := sync.NewCond(&rf.mu)
		awakenAll(&rf.mu, cond, rf.heartBeatTimer)
		rf.sendEntryToAll(&agree, &sent, cond)					//这里详细说一下

		rf.mu.Lock()
		for agree + 1 <= len(rf.peers) / 2 && sent < len(rf.peers) - 1 &&
			time.Now().Sub(rf.preTime) < rf.heartBeatTimer && rf.state == Leader {
			cond.Wait()
		}

		if rf.state != Leader {
			DPrintf("Leader    %d term %d  " + "state changed",
				rf.me, rf.currentTerm)
			rf.mu.Unlock()
			break
		}

		if agree + 1 > len(rf.peers) / 2 {
			DPrintf("Leader    %d term %d  " + "get enough agreement reply on time",
				rf.me, rf.currentTerm)
			rf.newLeaderCommitIndex()						// 当收到大多数的赞成时，我们给出最新的提交点
		} else {
			DPrintf("Leader    %d term %d  " + "fail to get enough reply on time",
				rf.me, rf.currentTerm)
		}

		preTime := rf.preTime
		heartBeatTimer := rf.heartBeatTimer
		rf.mu.Unlock()

		if time.Now().Sub(preTime) < heartBeatTimer {
			time.Sleep(heartBeatTimer - time.Now().Sub(preTime))
		}
	}
}

func (rf *Raft) sendEntryToAll(agree *int, sent *int, cond *sync.Cond)  {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()

		if rf.nextIndex[i] <= rf.lastIncludedIndex {			//我对你的的预期小于等于快照点，就让你安装快照
			rf.mu.Unlock()
			go rf.sendInstallSnapshotToPeer(i, agree, sent, cond)
		} else {												//大于快照点，则发送日志项或者心跳项
			rf.mu.Unlock()
			go rf.sendAppendEntriesToPeer(i, agree, sent, cond)
		}
	}
}

func (rf *Raft) sendAppendEntriesToPeer(peerId int, agree *int, sent *int, cond *sync.Cond)  {
	rf.mu.Lock()

	if rf.state != Leader {
		DPrintf("Leader    %d term %d  "+"state changed before AE to %d  ",
			rf.me, rf.currentTerm, peerId)						// 发送前的状态检查
		*sent++													// 下面基本同Candidate 流程，不再赘述
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}

	args := rf.newAppendEntriesArgs(peerId)
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(peerId, &args, &reply)
	if !ok {
		rf.mu.Lock()
		*sent++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()

	if rf.state != Leader || rf.currentTerm != args.Term {
		DPrintf("Leader    %d term %d  " + "state changed after AE to %d",
			rf.me, rf.currentTerm, peerId)
		*sent++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		*agree++
		rf.nextIndex[peerId] = args.PreLogIndex + len(args.Entries) + 1  //这里我们不用len（rf.log)的原因是无法确定在发送报文
		rf.matchIndex[peerId] = args.PreLogIndex + len(args.Entries)    //到接受这段时间内，是否有新的Command到达了Leader
	} else if reply.Term > rf.currentTerm {
		DPrintf("Leader    %d term %d  " + "find larger Term from %d",
			rf.me, rf.currentTerm, peerId)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist() 													//第六次持久化
		rf.state = Follower
	} else {
		rf.handleInconsistency(peerId, args, reply)				//如果不成功，并且不是因为任期的缘故，我们要日志一致性的处理
	}															//就是找到第一个不一致的日志索引
	*sent++
	cond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshotToPeer(peerId int, agree *int, sent *int, cond *sync.Cond)  {
	rf.mu.Lock()

	if rf.state != Leader {														// 发送前例行的状态检查
		DPrintf("Leader    %d term %d  "+"state changed before AE to %d  ",
			rf.me, rf.currentTerm, peerId)
		*sent++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}

	args := rf.newInstallSnapshotArgs()
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(peerId, &args, &reply)
	if !ok {
		rf.mu.Lock()
		*sent++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != args.Term {      	//例行判断自己有没有处理报文的能力
		DPrintf("Leader    %d term %d  " + "state changed after ISS to %d",
			rf.me, rf.currentTerm, peerId)
		*sent++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm {							//发现更大的任期，自动失败，改状态，身份
		DPrintf("Leader    %d term %d  " + "ISS find larger Term from %d",
			rf.me, rf.currentTerm, peerId)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist() 											//第八次持久化
		rf.state = Follower
	} else {
		*agree++
		rf.nextIndex[peerId] = args.LastIncludedIndex + 1      //否则，快照安装成功，下一次从快照点的下一个索引开始同步
		rf.matchIndex[peerId] = args.LastIncludedIndex
	}
	*sent++
	cond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) newInstallSnapshotArgs() InstallSnapshotArgs {

	args := InstallSnapshotArgs{
		Term: rf.currentTerm,
		LeaderId:  rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.lastIncludedTerm,
		Data:  rf.persister.ReadSnapshot(),
	}

	return args
}

func (rf *Raft) newAppendEntriesArgs(peerId int) AppendEntriesArgs {
	var entries []LogEntry

	if rf.nextIndex[peerId] <= rf.getLastLog().Index{
		entries = append(entries, rf.log[rf.getIndexAtLog(rf.nextIndex[peerId]):]...)
	}

	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PreLogIndex: rf.log[rf.getIndexAtLog(rf.nextIndex[peerId]) - 1].Index,
		PreLogTerm: rf.log[rf.getIndexAtLog(rf.nextIndex[peerId]) - 1].Term,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) handleInconsistency(peerId int, args AppendEntriesArgs, reply AppendEntriesReply)  {
	if reply.CTerm == -1 {											//这是请求的报文，在快照建立时间之前的。
		rf.nextIndex[peerId] = reply.CIndex							//我们直接改成follower的下一个快照点
	} else if reply.ClastLogIndex < args.PreLogIndex {				//对于冲突日志索引大于快照点，小于对比点的
		rf.nextIndex[peerId] = reply.ClastLogIndex + 1				//我们下一次，对比冲突索引即可
	} else {
		lastIndexOfCTerm := 0									//对于任期不一致的，我们在自己的索引处找到该任期的最后的
		l, r := rf.lastIncludedIndex, rf.getLastLog().Index					//索引，再去和接受方同步
		for l < r {
			mid := l + (r - l + 1) / 2
			if (reply.CTerm < rf.log[rf.getIndexAtLog(mid)].Term) {
				r = mid - 1
			} else {
				l = mid
			}
		}
		if (rf.log[rf.getIndexAtLog(r)].Term == reply.CTerm) {
			lastIndexOfCTerm = r
		}

		if lastIndexOfCTerm == 0 {	                          //如果我的日志中没有该任期，那么从接受方发来的冲突索引处开始同步
			rf.nextIndex[peerId] = reply.CIndex
		} else {
			rf.nextIndex[peerId] = lastIndexOfCTerm
		}
	}
}

func (rf *Raft) newLeaderCommitIndex()  {
	// 提交点的计算很简单，只要leader确认绝大多数的matchIndex匹配的log索引项即可
	N := rf.commitIndex + 1
	for {
		agree := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N {
				agree++
			}
		}
		if (agree + 1) <= len(rf.peers) / 2 {
			break
		}
		N++
	}
	N--

	//对于新提交点的要求，大于之前的提交点并且新提交点不能是之前任期的产物（做好你本任期的工作，别瞎操心）
	if N > rf.commitIndex && rf.log[rf.getIndexAtLog(N)].Term == rf.currentTerm {
		rf.commitIndex = N
		DPrintf("Leader    %d term %d  " + "push new commit index %d",
			rf.me, rf.currentTerm, rf.commitIndex)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.ClastLogIndex = rf.getLastLog().Index			//细节，如果接受方比发送方预期的要短
	if args.Term < rf.currentTerm {							//那么要从接受方最后一个索引处同步
		reply.Success = false								//对于任期比我们小的，一律拒绝
		return
	}

	rf.preTime = time.Now()
	rf.state = Follower										//持久化的原因，此时发来的Term一定是大于等于我们的rf.currentTerm
	rf.currentTerm = args.Term								//第五次持久化
	rf.persist()

	if args.PreLogIndex < rf.lastIncludedIndex  {			//args认为我们应该是还没建立快照。很可能是一条过期的报文
		reply.Success = false								//直接告诉发送方，我们已经建立了快照，让他从下一个开始发
		reply.CTerm = -1
		reply.CIndex = rf.lastIncludedIndex + 1
		return
	}

	if rf.getLastLog().Index >= args.PreLogIndex &&					//接受方的日志要比发送方长，并且对比索引和日志最后项任期一致
		rf.log[rf.getIndexAtLog(args.PreLogIndex)].Term == args.PreLogTerm {	//说明至少接受方在preLogIndex处还没有跳到另一个任期，小错误
		conflict := false
		endIndexOfLog := Min(args.PreLogIndex + len(args.Entries), rf.getLastLog().Index)

		for i := args.PreLogIndex + 1; i <= endIndexOfLog; i++ {				//如果在对比索引到最后索引过程中任期有错，说明日志冲突
			if rf.log[rf.getIndexAtLog(i)].Term != args.Entries[i - args.PreLogIndex - 1].Term {
				conflict = true
				break
			}
		}

		if conflict ||		//当日志冲突，或者我的日志没有发送方长，说明我可能丢失了一些信息。我们把日志从对比索引+1处向后全部替换
			(rf.getIndexOfLastLog() - rf.getIndexAtLog(args.PreLogIndex)) <= len(args.Entries) {
			rf.log = rf.log[:rf.getIndexAtLog(args.PreLogIndex + 1)]
			rf.log = append(rf.log, args.Entries...)
		}
		rf.persist()							//第七次持久化

		rf.commitIndex = Min(args.LeaderCommit, args.PreLogIndex + len(args.Entries)) //这里提交完日志后，我们重新计算提交点。
		DPrintf("Follower  %d  term %d  " + "get new commitIndex %d", rf.me, rf.currentTerm, rf.commitIndex)
		DPrintf("Follower  %d  term %d  " + "leaderCommit is %d, args length is %d", rf.me,
			rf.currentTerm, args.LeaderCommit, args.PreLogIndex + len(args.Entries))
		reply.Success = true

	} else {
		//大规模的不一致，主要是索引处的任期不一致。
		DPrintf("Follower  %d  term %d  " + "find AE Inconsistency", rf.me, rf.currentTerm)

		cIndex := rf.lastIncludedIndex
		cTerm := 0
		if args.PreLogIndex <= rf.getLastLog().Index {     			//如果是对比索引处的任期不一致，那我们的目的就是把
			cTerm = rf.log[rf.getIndexAtLog(args.PreLogIndex)].Term		//自己对比索引处的任期以及首出现该任期的索引告诉
			l , r:= rf.lastIncludedIndex, rf.getLastLog().Index									//发送方
			for l < r {
				mid := l + (r - l) / 2
				if rf.log[rf.getIndexAtLog(mid)].Term < cTerm {
					l = mid + 1
				} else {
					r = mid
				}
			}
			if (rf.log[rf.getIndexAtLog(l)].Term == cTerm) {
				cIndex = l
			}
		}
		reply.CIndex = cIndex					//为什么要发冲突任期的第一个索引? 因为我们不确定接受方的日志里是
		reply.CTerm = cTerm							//否有该任期。如没有，则让发送方从这个索引处开始同步
		reply.Success = false
		return
	}
}


func (rf *Raft) InstallSnapshot (args *InstallSnapshotArgs, reply *InstallSnapshotReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}						//例行的任期检查

	rf.preTime = time.Now()
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.persist() 											//第九次持久化

	if rf.lastIncludedIndex >= args.LastIncludedIndex {	   //例行的快照点检查，判断是不是旧的报文
		return
	}

	log := make([]LogEntry, 0)								//从接收的这条快照命令起
	log = append(log, LogEntry{								//重做follower的日志。日志第一条就是快照命令
		Index: args.LastIncludedIndex,
		Term: args.LastIncludedTerm,
		Command: nil,
	})

	if args.LastIncludedIndex < rf.getLastLog().Index {		//如果需要建日志处的索引小于日志的最后索引,那么我们还得将
		log = append(log, rf.log[rf.getIndexAtLog(args.LastIncludedIndex) + 1 :]...)	//快照点后面的索引追加入日志
	}

	rf.log = log
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.serializeState(), args.Data)  //快照建立成功，我们进行自身的快照持久化
	rf.lastApplied = args.LastIncludedIndex

	rf.applyCh <- ApplyMsg{												//写入建立快照的命令
		CommandValid: false,											//命令的内容就是需要被写入快照的字节数组
		CommandIndex: -1,
		CommandTerm: rf.currentTerm,
		Command: rf.persister.ReadSnapshot(),
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
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

func (rf *Raft) applyLog()  {
	for {

		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			break
		}

		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.log[rf.getIndexAtLog(i)].Index,
					CommandTerm:  rf.currentTerm,
					Command:  rf.log[rf.getIndexAtLog(i)].Command,
				}
				DPrintf(" peer %d apply Index %d", rf.me, i)
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	//对于所有的server，首次启动或者或者故障恢复时，都重置为Follower
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.preTime = time.Now()
	rf.electionTimer = time.Duration(200 + rand.Intn(200)) * time.Millisecond
	rf.heartBeatTimer = 100 * time.Millisecond

	// 从故障中恢复，包括任期，投票，日志，快照
	rf.readPersist(persister.ReadRaftState())

	rf.lastApplied = rf.lastIncludedIndex // 由于快照的存在，已应用的索引应该是快照的最后一个索引项
	if len(rf.log) == 0 {					//我们消耗掉索引0不用，作为哨兵
		rf.log = append(rf.log, LogEntry{
			Term: -1,
			Index: 0,
			Command: nil,
		})
	}

	go rf.run()
	go rf.applyLog()

	return rf
}
