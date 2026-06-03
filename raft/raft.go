package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// create a new Raft server.
//		rf = Make(...)
// start agreement on a new log entry
//		rf.Start(command interface{}) (index, term, isleader)
// ask a Raft for its current term, and whether it thinks it is leader
//		rf.GetState() (term, isLeader)
// each time a new entry is committed to the log, each Raft peer should send
// an ApplyMsg to the service (or tester) in the same server.
//		ApplyMsg
//

import (
	"bytes"
	"cs345/labrpc"
	"cs345/labgob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term   int
	Command interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

const heartbeatInterval = 120 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg // Channel for the commit to the state machine
	applyCond *sync.Cond

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers

	currentTerm int
	votedFor    int
	log         []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Election/heartbeat state
	role 	   int
	electionResetTime time.Time
	electionTimeout time.Duration
	dead	  int32

}

func randomElectionTimeout() time.Duration {
	return time.Duration(400+rand.Intn(401)) * time.Millisecond
}

// only call while holding rf.mu
func(rf * Raft) resetElectionTimerLocked() {
	rf.electionResetTime = time.Now()
	rf.electionTimeout = randomElectionTimeout()
}

// only call while holding rf.mu
func (rf * Raft) becomeFollowerLocked(term int) {
	rf.role = Follower

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}

	rf.resetElectionTimerLocked()
}

// real log start at index 1, log[0] just a dummy entry to simplify index math
func (rf * Raft) lastLogIndexLocked() int {
	return len(rf.log) - 1
}

func (rf * Raft) lastLogTermLocked() int {
	return rf.log[rf.lastLogIndexLocked()].Term
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log

	if len(rf.log) == 0 {
		rf.log = []LogEntry{{Term: 0}}
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3, 4).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3, 4).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
		rf.persist()
	}

	// Election restructions
	// Server only votes for candidate whose log is at least as up to date
	myLastTerm := rf.lastLogTermLocked()
	myLastIndex := rf.lastLogIndexLocked()

	candidateLogIsUpToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if candidateLogIsUpToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		rf.role = Follower
		rf.resetElectionTimerLocked()
		rf.persist()
		reply.VoteGranted = true
	}
	reply.Term = rf.currentTerm
}

// AppendEntries used for heartbeats and log replication
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = 1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
		rf.persist()
	} else {
		rf.role = Follower
		rf.resetElectionTimerLocked()
	}
	reply.Term = rf.currentTerm

	// follower must have PrevLogIndex
	if args.PrevLogIndex > rf.lastLogIndexLocked() {
		reply.ConflictIndex = rf.lastLogIndexLocked() + 1
		return
	}

	// followers term at PrevLogIndex has to match leader's
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		conflictIndex := args.PrevLogIndex
		for conflictIndex > 1 && rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}

		reply.ConflictIndex = conflictIndex
		return
	}

	// append new entries, delete conlicts if needed
	changedLog := false
	insertIndex := args.PrevLogIndex + 1

	for i := 0; i < len(args.Entries); i++ {
		logIndex := insertIndex + i

		if logIndex <= rf.lastLogIndexLocked() {
			if rf.log[logIndex].Term != args.Entries[i].Term {
				rf.log = rf.log[:logIndex]
				rf.log = append(rf.log, args.Entries[i:]...)
				changedLog = true
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			changedLog = true
			break
		}
	}

	if changedLog {
		rf.persist()
	}

	// update commit index from leader
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndexLocked())
		rf.applyCond.Broadcast()
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf * Raft) becomeLeaderLocked() {
	rf.role = Leader
	lastIndex := rf.lastLogIndexLocked()

	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}

	rf.matchIndex[rf.me] = lastIndex
	rf.nextIndex[rf.me] = lastIndex + 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	if rf.role == Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	rf.role = Candidate
	rf.currentTerm++
	termStarted := rf.currentTerm
	rf.votedFor = rf.me
	rf.resetElectionTimerLocked()
	
	lastLogIndex := rf.lastLogIndexLocked()
	lastLogTerm := rf.lastLogTermLocked()

	rf.persist()

	votes := 1

	wonImmediately := votes > len(rf.peers)/2
	if wonImmediately {
		rf.becomeLeaderLocked()
	}

	rf.mu.Unlock()

	if wonImmediately {
		rf.broadcastAppendEntries()
		return
	}

	args := RequestVoteArgs{
		Term: termStarted,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)

			if !ok {
				return
			}

			becameLeader := false
			rf.mu.Lock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.persist()
			} else if rf.role == Candidate && rf.currentTerm == termStarted && reply.VoteGranted {
				votes++

				if votes > len(rf.peers)/2 {
					rf.becomeLeaderLocked()
					becameLeader = true
				}
		}

		rf.mu.Unlock()

		if becameLeader {
			rf.broadcastAppendEntries()
		}
	}(peer)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()

	if rf.role != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go rf.replicateToPeer(peer)
	}
}

func (rf *Raft) replicateToPeer(server int) {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		term := rf.currentTerm

		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}

		if rf.nextIndex[server] > rf.lastLogIndexLocked()+1 {
			rf.nextIndex[server] = rf.lastLogIndexLocked() + 1
		}

		nextIndex := rf.nextIndex[server]
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.log[prevLogIndex].Term

		entries := make([]LogEntry, len(rf.log[nextIndex:]))
		copy(entries, rf.log[nextIndex:])

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		rf.mu.Unlock()

		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			newMatch := args.PrevLogIndex + len(args.Entries)

			if newMatch > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatch
			}

			rf.nextIndex[server] = rf.matchIndex[server] + 1

			rf.advanceCommitIndexLocked()

			rf.mu.Unlock()
			return
		}

		// AppendEntries failed bc follower log didn't match
		// back up nextIndex and retry
		if reply.ConflictTerm != -1 {
			lastIndexOfTerm := -1

			for i := rf.lastLogIndexLocked(); i >= 1; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					lastIndexOfTerm = i
					break
				}
			}

			if lastIndexOfTerm != -1 {
				rf.nextIndex[server] = lastIndexOfTerm + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}

		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) advanceCommitIndexLocked() {
	if rf.role != Leader {
		return
	}

	// try to move commitIndex forward
	// only commit entries from the current term directly
	for n := rf.lastLogIndexLocked(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		count := 1 // leader itself

		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			return
		}
	}
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		timedOut := rf.role != Leader &&
			time.Since(rf.electionResetTime) >= rf.electionTimeout
		rf.mu.Unlock()

		if timedOut {
			rf.startElection()
		}
	}
}

func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		time.Sleep(heartbeatInterval)
		rf.broadcastAppendEntries()
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		for !rf.killed() && rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		rf.lastApplied++
		index := rf.lastApplied
		entry := rf.log[index]

		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: index,
		}
	}
}



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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	if rf.killed() || rf.role != Leader {
		term := rf.currentTerm
		rf.mu.Unlock()
		return -1, term, false
	}

	term := rf.currentTerm

	entry := LogEntry{
		Term:    term,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	index := rf.lastLogIndexLocked()

	rf.persist()

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.advanceCommitIndexLocked()

	rf.mu.Unlock()

	// start replication right away don't for the next heartbeat
	rf.broadcastAppendEntries()

	return index, term, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&rf.dead, 1)

	if rf.applyCond != nil {
		rf.mu.Lock()
		rf.applyCond.Broadcast()
		rf.mu.Unlock()
	}
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3, 4).
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.role = Follower
	rf.resetElectionTimerLocked()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTicker()
	go rf.heartbeatTicker()
	go rf.applier()

	return rf
}
