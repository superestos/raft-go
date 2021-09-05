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
	"sync"
	"sync/atomic"

	"time"
	"math/rand"

	"6.824/labgob"
	"6.824/labrpc"
)

const electionTimeout = 150
const heartBeatTime = 100
const waitResultTime = 5
const applyInterval = 2

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandTerm  int
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntries struct {
	Command		interface{}
	Term		int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg		  // Message channel to the client
	notifyCommitCh chan bool

	latestCall time.Time

	currentTerm int
	votedFor int
	isLeader bool

	firstLogIndex int
	lastLogIndex int
	log []LogEntries
	
	lastApplied int
	commitIndex int

	nextIndex []int
	matchIndex []int

	lastIncludedIndex int
	lastIncludedTerm int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.isLeader
}

// lock hold
func (rf *Raft) termOfLog(index int) int {
	if index >= rf.firstLogIndex {
		return rf.log[index - rf.firstLogIndex].Term
	} else if index == 0 {
		return 0
	} else {
		return rf.lastIncludedTerm
	}
}

//lock hold
func (rf *Raft) trimLog(first int, last int) {
	start := rf.firstLogIndex
	if first > rf.firstLogIndex {
		start = first
	}

	end := rf.lastLogIndex
	if end > last {
		end = last
	}

	if start <= end {
		rf.log = rf.log[start - rf.firstLogIndex : end + 1 - rf.firstLogIndex]
		rf.firstLogIndex = start
		rf.lastLogIndex = end
	} else {
		rf.log = make([]LogEntries, 0)
		rf.firstLogIndex = first
		rf.lastLogIndex = first - 1
	}	
}

type PersistedState struct {
	CurrentTerm int
	VotedFor int
	FirstLogIndex int
	LastLogIndex int
	Log []LogEntries
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	state := PersistedState{rf.currentTerm, rf.votedFor, rf.firstLogIndex, rf.lastLogIndex, rf.log}
	e.Encode(state)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) saveSnapshot(data []byte) {
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), data)
	rf.persist()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	state := PersistedState{}
	d.Decode(&state)

	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.firstLogIndex = state.FirstLogIndex
	rf.lastLogIndex = state.LastLogIndex
	rf.log = state.Log
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	/*
	if lastIncludedIndex < rf.lastApplied {
		//DPrintf("Warning: server %d, CondInstallSnapshot() try to roll back, %d, %d\n", rf.me, lastIncludedIndex, rf.lastApplied)
		return false
	}
	*/
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex

	rf.lastApplied = lastIncludedIndex

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastIncludedTerm = rf.termOfLog(index)
	rf.lastIncludedIndex = index

	if rf.lastLogIndex < index || rf.lastApplied < index {
		DPrintf("Warning: server %d, Snapshot() trim log, %d, %d, %d\n", rf.me, rf.lastLogIndex, rf.lastApplied, index)
	}

	rf.trimLog(index + 1, rf.lastLogIndex)

	rf.saveSnapshot(snapshot)
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int

	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VotedGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	alreadyVoted := rf.currentTerm == args.Term && (rf.votedFor != -1 || rf.votedFor != args.CandidateId)
	notUpToDate := (rf.termOfLog(rf.lastLogIndex) > args.LastLogTerm) || (rf.termOfLog(rf.lastLogIndex) == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex)
	
	if rf.currentTerm > args.Term || alreadyVoted || notUpToDate {
		reply.VotedGranted = false
		if rf.currentTerm < args.Term {
			rf.becomeFollower(args.Term, -1)
		}
	} else {
		rf.latestCall = time.Now()
		reply.VotedGranted = true
		rf.becomeFollower(args.Term, args.CandidateId)
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int

	Entries []LogEntries
}

type AppendEntriesReply struct {
	Term int
	Success bool

	ConflictTerm int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}
	rf.becomeFollower(args.Term, args.LeaderId)
	rf.latestCall = time.Now()

	if rf.lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex + 1
		return
	}

	// inconsistence of log and snapshot due to crash
	if rf.lastIncludedIndex + 1 < rf.firstLogIndex && rf.lastIncludedIndex < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}

	if rf.termOfLog(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.termOfLog(args.PrevLogIndex)
		ind := args.PrevLogIndex - 1
		for reply.ConflictTerm == rf.termOfLog(ind) && rf.firstLogIndex <= ind {
			ind -= 1
		}
		reply.ConflictIndex = ind + 1

		if args.PrevLogIndex <= rf.lastApplied {
			DPrintf("Warning: server %d, AppendEntries() trim log that applied, %d, %d\n", rf.me, args.PrevLogIndex - 1, rf.lastApplied)
		}

		rf.trimLog(rf.firstLogIndex, args.PrevLogIndex - 1)
		rf.persist()
		return
	}

	reply.Success = true

	if len(args.Entries) == 0 || (args.PrevLogIndex + len(args.Entries) <= rf.lastLogIndex && rf.termOfLog(args.PrevLogIndex + len(args.Entries)) == args.Entries[len(args.Entries) - 1].Term) {
		rf.updateFollowerCommit(args.LeaderCommit, args.PrevLogIndex + len(args.Entries))
		return
	}

	rf.trimLog(rf.firstLogIndex, args.PrevLogIndex)
	rf.log = append(rf.log, args.Entries...)
	rf.lastLogIndex += len(args.Entries)
	rf.persist()

	rf.updateFollowerCommit(args.LeaderCommit, rf.lastLogIndex)
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm int

	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.becomeFollower(args.Term, args.LeaderId)
	rf.latestCall = time.Now()
	
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	
	if args.LastIncludedIndex <= rf.lastLogIndex && rf.termOfLog(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.trimLog(args.LastIncludedIndex + 1, rf.lastLogIndex)
		rf.saveSnapshot(args.Data)
	} else {
		if args.LastIncludedIndex < rf.lastApplied {
			DPrintf("Warning: server %d, InstallSnapshot() trim log that applied, %d, %d\n", rf.me, args.LastIncludedIndex, rf.lastApplied)
		}
	
		rf.trimLog(args.LastIncludedIndex + 1, args.LastIncludedIndex)
		rf.saveSnapshot(args.Data)
	}

	//rf.lastApplied = args.LastIncludedIndex
	rf.mu.Unlock()

	msg := ApplyMsg{}
	msg.SnapshotValid = true
	msg.SnapshotTerm = args.LastIncludedTerm
	msg.SnapshotIndex = args.LastIncludedIndex
	msg.Snapshot = args.Data

	rf.applyCh <- msg
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

// lock hold
func (rf *Raft) makeAppendEntriesArgs(prevLogIndex int, numEntries int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.termOfLog(prevLogIndex)
	args.LeaderCommit = rf.commitIndex	

	if numEntries > 0 {
		args.Entries = rf.log[prevLogIndex + 1 - rf.firstLogIndex: prevLogIndex + numEntries + 1 - rf.firstLogIndex]
	} else {
		args.Entries = nil
	}
	return args
}

// lock hold
func (rf *Raft) appendMatchedLog(server int, numEntries int) {
	if rf.nextIndex[server] >= rf.firstLogIndex {
		newArgs := rf.makeAppendEntriesArgs(rf.nextIndex[server] - 1, numEntries)
		go rf.handleAppendEntries(server, &newArgs)
	} else {
		go rf.handleInstallSnapshot(server)
	}
}

// receive append entries reply
func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term, -1)
	}

	if !ok || !rf.isLeader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		if rf.matchIndex[server] <= args.PrevLogIndex + len(args.Entries) {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

			rf.updateLeaderCommit()
		}

		if rf.nextIndex[server] <= rf.lastLogIndex {
			rf.appendMatchedLog(server, rf.lastLogIndex - rf.nextIndex[server] + 1)
		}
	} else {
		rf.nextIndex[server] = reply.ConflictIndex
		if reply.ConflictTerm != -1 {
			for i := rf.lastLogIndex; i >= rf.firstLogIndex; i-- {
				if rf.termOfLog(i) == reply.ConflictTerm {
					rf.nextIndex[server] = i + 1
					break
				}
			}
		}

		rf.appendMatchedLog(server, 0)
	}	
}

func (rf *Raft) handleInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.persister.ReadSnapshot()}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term, -1)
	}

	if !rf.isLeader || !ok || rf.currentTerm != args.Term {
		return
	}

	if rf.matchIndex[server] < args.LastIncludedIndex {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex

		rf.updateLeaderCommit()
	}

	if rf.nextIndex[server] <= rf.lastLogIndex {
		rf.appendMatchedLog(server, rf.lastLogIndex - rf.nextIndex[server] + 1)
	}
}

//lock hold
func (rf *Raft) updateLeaderCommit() {
	for n := rf.commitIndex + 1; n <= rf.lastLogIndex; n++ {
		if rf.termOfLog(n) != rf.currentTerm {
			continue
		}
		
		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= n {
				count += 1
			}
		}

		if count > len(rf.peers) / 2 {
			rf.commitIndex = n
			rf.notifyCommit()
		} else {
			break
		}
	}
}

//lock hold
func (rf *Raft) updateFollowerCommit(leaderCommit int, lastLogIndex int) {
	if leaderCommit > rf.commitIndex {
		prevCommitIndex := rf.commitIndex

		if leaderCommit > lastLogIndex {
			rf.commitIndex = lastLogIndex
		} else {
			rf.commitIndex = leaderCommit
		}

		if prevCommitIndex != rf.commitIndex {
			rf.notifyCommit()
		}
	}
}

func (rf *Raft) notifyCommit() {
	select {
		case rf.notifyCommitCh <- true:
		default:
	}
}

func (rf *Raft) sendHeartBeat() {
	ticker := time.NewTicker(heartBeatTime * time.Millisecond)

	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	for !rf.killed() {

		rf.mu.Lock()
		if !rf.isLeader || term != rf.currentTerm {
			rf.mu.Unlock()
			break
		}

		args := rf.makeAppendEntriesArgs(rf.lastLogIndex, 0)
		rf.mu.Unlock()
		
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.handleAppendEntries(i, &args)
			}
		}

		<-ticker.C
	}	
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs, server int, voteCount *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term, -1)
		*voteCount = -len(rf.peers)
	}

	if reply.VotedGranted {
		*voteCount += 1
	}	
}

//lock hold
func (rf *Raft) becomeFollower(term int, leaderId int) {
	rf.isLeader = false
	rf.currentTerm = term
	rf.votedFor = leaderId
	rf.nextIndex = nil
	rf.matchIndex = nil

	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.isLeader = true
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
	}

	go rf.sendHeartBeat()
}

// receive request vote reply
func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLogIndex, rf.termOfLog(rf.lastLogIndex)}

	// inconsistence of log and snapshot due to crash
	if rf.lastIncludedIndex + 1 < rf.firstLogIndex {
		args.LastLogIndex = rf.lastIncludedIndex
		args.LastLogTerm = rf.lastIncludedTerm
	}

	rf.mu.Unlock()

	ticker := time.NewTicker(waitResultTime * time.Millisecond)
	voteCount := 1
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.handleRequestVote(&args, i, &voteCount)
		}
	}

	<-ticker.C

	rf.mu.Lock()
	if voteCount > len(rf.peers) / 2 && args.Term == rf.currentTerm && rf.votedFor == rf.me {
		rf.becomeLeader()
	}
	rf.mu.Unlock()
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
	rf.mu.Lock()

	defer func() {
		if rf.isLeader {
			rf.lastLogIndex += 1
			rf.log = append(rf.log, LogEntries{command, rf.currentTerm})
			rf.persist()

			args := rf.makeAppendEntriesArgs(rf.lastLogIndex - 1, 1)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] == rf.lastLogIndex - 1 {
					go rf.handleAppendEntries(i, &args)
				}
			}
		}

		rf.mu.Unlock()
	}()

	return rf.lastLogIndex + 1, rf.currentTerm, rf.isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		prevlatestCall := rf.latestCall
		rf.mu.Unlock()

		electionTimeout := time.Millisecond * time.Duration(electionTimeout + rand.Intn(2 * electionTimeout))
		time.Sleep(electionTimeout - time.Now().Sub(prevlatestCall))

		rf.mu.Lock()
		if prevlatestCall == rf.latestCall && !rf.isLeader {
			rf.mu.Unlock()
			rf.becomeCandidate()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applyStateMachine() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1

			msg := ApplyMsg{}
			msg.CommandValid = rf.lastApplied >= rf.firstLogIndex && rf.lastApplied <= rf.lastLogIndex
			if msg.CommandValid {
				msg.CommandTerm = rf.log[rf.lastApplied - rf.firstLogIndex].Term
				msg.CommandIndex = rf.lastApplied
				msg.Command = rf.log[rf.lastApplied - rf.firstLogIndex].Command
			} else {
				rf.lastApplied -= 1
				break
			}

			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()

		select {
		case <- time.After(applyInterval * time.Millisecond):
		case <- rf.notifyCommitCh:
		}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh

	rf.notifyCommitCh = make(chan bool, 1)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.isLeader = false
	rf.latestCall = time.Now()

	rf.log = make([]LogEntries, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.firstLogIndex = 1
	rf.lastLogIndex = 0

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyStateMachine()

	return rf
}
