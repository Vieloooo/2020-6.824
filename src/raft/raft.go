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
	//"bytes"
	"math/rand"
	//"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//some data for election
	eleMu         sync.Mutex
	currentTerm   int
	voteFor       int
	role          int
	loopStartTime time.Time
}

// role
const (
	Follower = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.role==Leader{
		isleader= true
	}
	return term, isleader
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
	// e.Encode(rf.yyy)persister
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

// vote args and replies
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	/* 2b
	LastLogIndex int
	LastLogTerm int
	*/
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// append args and replies
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.currentTerm > args.Term {
		return
	}
	rf.eleMu.Lock()
	rf.loopStartTime = time.Now()
	rf.eleMu.Unlock()
	reply.Success = true
	rf.currentTerm = args.Term
	rf.voteFor = args.LeaderId
	rf.role = Follower

}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// for lab 2a, without log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		DPrintf("x to F %d", rf.me)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.voteFor = -1
	}
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		rf.loopStartTime = time.Now() // 为其他人投票，那么重置自己的下次投票时间
	}
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

	return index, term, isLeader
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
	// my code
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.role = Follower
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//election routime
	go rf.electionloop()
	// leaderbeat routine
	go rf.leaderBeat()
	DPrintf("a node start,my id is %d\n", rf.me)
	// sync routine
	return rf
}
func (rf *Raft) electionloop() {
	for !rf.killed() {
		now := time.Now()
		time.Sleep(time.Millisecond * 10)
		timeLimit := time.Duration(200+rand.Int31n(300)) * time.Millisecond
		rf.eleMu.Lock()
		timePast := now.Sub(rf.loopStartTime)
		rf.eleMu.Unlock()
		//timeout
		if timeLimit < timePast {
			//DPrintf("timeout %v,me: %d,term: %d", timeLimit, rf.me, rf.currentTerm)
			rf.mu.Lock()
			switch rf.role {
			case Follower:
				DPrintf("F to C, %d", rf.me)
				rf.role = Candidate
				rf.voteFor = rf.me
				rf.mu.Unlock()

			case Candidate:
				// restart election
				rf.eleMu.Lock()
				rf.loopStartTime = time.Now()
				rf.eleMu.Unlock()
				rf.currentTerm++
				rf.voteFor = rf.me
				newTerm := rf.currentTerm
				//ask for vote
				args := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				//unlock to send vote rpc
				rf.mu.Unlock()
				voteReply := make(chan *RequestVoteReply, len(rf.peers))
				for id := 0; id < len(rf.peers); id++ {
					go func(id int) {
						if id == rf.me {
							return
						}
						reply := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &args, &reply); ok {
							voteReply <- &reply
						} else {
							DPrintf("internet error when %d call %d", rf.me, id)
							voteReply <- &RequestVoteReply{Term: -1, VoteGranted: false}
						}

					}(id)
				}
				//caculate the result
				voteDone := 1
				voteMe := 1
				for {
					select {
					case result := <-voteReply:
						//DPrintf("get a vote reply %v",result)
						voteDone++
						if result.Term != -1 {
							if result.VoteGranted {
								voteMe++
							}
							if newTerm < result.Term {
								newTerm = result.Term
							}
						}
					}
					if voteDone == len(rf.peers) || voteMe > len(rf.peers)/2 {
						break
					}
				}
				DPrintf(" i'm %d, vote done, get %d out of %d ", rf.me, voteMe, voteDone)
				//analyze the vote result
				// re lock
				rf.mu.Lock()

				// check role
				if rf.role == Candidate {
					if newTerm > rf.currentTerm {
						DPrintf("C to F, %d\n", rf.me)
						rf.role = Follower
						rf.voteFor = -1
						rf.currentTerm = newTerm

					} else {
						if voteMe > len(rf.peers)/2 {
							DPrintf("C to L, term: %d,%d\n", rf.currentTerm,rf.me)
							rf.role = Leader
							rf.loopStartTime = time.Now()

						} else {

						}
					}
				}
				rf.mu.Unlock()
			case Leader:
				//DPrintf("%d still leader",rf.me)
				rf.mu.Unlock()
				//time.Sleep(10 *time.Millisecond)
			}
		}
	}
	time.Sleep(1000*time.Millisecond)
}
func (rf *Raft) leaderBeat() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 100)
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			continue
		}
		LPrintf("*****\n %d is L,start beat,role:%d,term: %d", rf.me,rf.role,rf.currentTerm)
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.broadcast(i)
		}
	}
	time.Sleep(1000*time.Millisecond)
}

func (rf *Raft) broadcast(id int) {
	args := AppendEntriesArgs{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	rf.mu.Unlock()
	LPrintf("%d lord %d, term:%d", rf.me, id, args.Term)
	go func() {
		reply := AppendEntriesReply{}
		reply.Success= false
		ok := rf.sendAppendEntries(id, &args, &reply)
		if ok {
			if !reply.Success {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					LPrintf("L to F, %d", rf.me)
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.voteFor = -1
				}
			}
		}else{
			LPrintf("%d can't lord %d,net err",rf.me,id)
		}
	}()
		//time.Sleep(100*time.Millisecond)
}
