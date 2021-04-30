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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



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
type Log struct{
	Term int
	Cmd	string
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	eleMu 		sync.Mutex			// lock currentTerm, voteFor, lastAppendTime
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// in all server
	currentTerm int
	voteFor int
	// temp log
	logEntries []Log
	// valatile on all server
	commitIndex int
	lastApplied int
	role int
	loopStartTime time.Time
	// leader data
	nextIndex []int
	matchIndex []int

}

func (rf *Raft) sync() {
	for !rf.killed(){
		func(){
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.commitIndex>rf.lastApplied{
				fmt.Printf("add a commitindex ")
				rf.lastApplied++
			}else{
				time.Sleep(10*time.Millisecond)
			}
		}()

	}
}


const(
	Follower = iota
	Candidate
	Leader
)
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader= rf.role==Leader
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
	// e.Encode(rf.yyy)
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
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
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("%d get a vote req from %d",rf.me, args.CandidateId)
	if args.Term<rf.currentTerm{
		//DPrintf("refuse vote, my term %d is bigger than args  term %d",rf.commitIndex,args.Term)
		reply.VoteGranted= false
		reply.Term= rf.currentTerm
	}else{
		rf.currentTerm= args.Term
	}
	if rf.voteFor==-1 || rf.voteFor==args.CandidateId{
		if args.LastLogIndex==-1{
			//DPrintf("%d get a empty log vote from %d",rf.me, args.CandidateId)
			if len(rf.logEntries)==0{
				//DPrintf("%d vote for %d ",rf.me,args.CandidateId)
				reply.VoteGranted=true
				rf.voteFor=args.CandidateId
			}
		}else{
			if args.LastLogIndex+1>=len(rf.logEntries) && args.LastLogTerm>=rf.logEntries[len(rf.logEntries)-1].Term{
				//DPrintf("%d vote for %d ",rf.me,args.CandidateId)
				reply.VoteGranted= true
				rf.voteFor= args.CandidateId
			}
		}


	}
}
func (rf *Raft)AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success= false
	reply.Term= rf.currentTerm
	if rf.currentTerm>args.Term{
		return
	}
	if args.PreLogTerm>=len(rf.logEntries){
		//out of range
		return
	}
	if rf.logEntries[args.PrevLogIndex].Term!=args.PreLogTerm{
		return
	}
	if args.Term>rf.currentTerm{
		DPrintf("to F, %d",rf.me)
		rf.role= Follower
		rf.voteFor= args.LeaderId
	}
	rf.currentTerm= args.Term
	rf.eleMu.Lock()
	rf.loopStartTime= time.Now()
	rf.eleMu.Unlock()
	// append the log
	//  golang append() can cover the different log
	rf.logEntries=append(rf.logEntries[:args.PrevLogIndex],args.Entries...)
	// commit
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = args.LeaderCommitIndex
		if rf.GetLastLogIndex() < rf.commitIndex {
			rf.commitIndex = rf.GetLastLogIndex()
		}
	}
	reply.Success=true
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
func (rf *Raft)GetLastLogIndex()int{
	return len(rf.logEntries)-1
}
func (rf *Raft)GetLastLogTerm()int{
	if rf.GetLastLogIndex()>=0{
		return rf.logEntries[rf.GetLastLogIndex()].Term
	}
	return 0
}
// election loop
func (rf *Raft)election(){
	// every 10ms, check if the node lose communication
	//DPrintf("election loop start")
	for !rf.killed(){
		time.Sleep(5*time.Millisecond)
		//DPrintf("start check ")
		// new a election timelimit
		timeLimit:= time.Duration(200+rand.Int31n(200)) * time.Millisecond
		rf.eleMu.Lock()
		timePast:= time.Now().Sub(rf.loopStartTime)
		//DPrintf("time limit is %v,time past is %v",timeLimit,timePast)
		rf.eleMu.Unlock()
		if timePast>timeLimit{
			//DPrintf("timeout id:%d, term:%d, timelim:%v, timpss:%v",rf.me,rf.currentTerm,timeLimit,timePast)
			rf.mu.Lock()
			switch rf.role {
			// if follower lose comunication, to candidate
			case Follower:
					rf.role= Candidate
					rf.voteFor=rf.me
					rf.mu.Unlock()
					DPrintf("F to C, %d",rf.me)
			// if a candidate timeout, re-election
			case Candidate:
				rf.eleMu.Lock()
				rf.loopStartTime= time.Now()
				rf.eleMu.Unlock()
				rf.currentTerm++
				//DPrintf("new election, refresh timeout and term ++\n")

				rf.voteFor= rf.me
				newTerm:= rf.currentTerm
				//at the same time, the election state of node can be modified by others
				// ask for vote
				args:=RequestVoteArgs{
					Term: rf.currentTerm,
					CandidateId: rf.me,
					LastLogIndex:rf.GetLastLogIndex(),
					LastLogTerm: rf.GetLastLogTerm(),
				}
				//DPrintf("my vote args is %v",args)
				rf.mu.Unlock()// unlock the Mu before sending vote req to save locking time
				//DPrintf("i(%d) am candidate, start ask for vote ",rf.me)
				//use go routine to send vote req concurrency
				voteReply:= make(chan *RequestVoteReply,len(rf.peers))
				for id := 0; id < len(rf.peers); id++ {
					go func(id int) {
						if id == rf.me {
							return
						}
						reply:=RequestVoteReply{}
						if ok:=rf.sendRequestVote(id,&args,&reply) ; ok{
							voteReply <- &reply
						}else{
							DPrintf("internet error when %d call %d",rf.me,id)
							voteReply <-&RequestVoteReply{Term: -1,VoteGranted: false}
						}

					}(id)
				}
					//caculate the result
					voteDone:=1
					voteMe:=1
					for{
						select{
						case result:= <- voteReply:
							//DPrintf("get a vote reply %v",result)
							voteDone++
							if result.Term!=-1{
								if result.VoteGranted{
									voteMe++
								}
								if newTerm<result.Term{
									newTerm= result.Term
								}
							}
						}
						if voteDone==len(rf.peers)|| voteMe>len(rf.peers)/2{
							break
						}
					}
					//DPrintf(" i'm %d, vote done, get %d out of %d ",rf.me,voteMe,voteDone)
					//process the vote result
					//voteMe newTerm
					//rf.mu.Lock()
					//defer rf.mu.Unlock()
					// check the node role
					if rf.role==Candidate{
						//DPrintf("im still candidtate,my id: %d",rf.me)
						if newTerm>rf.currentTerm{
							rf.role= Follower
							rf.currentTerm= newTerm
							rf.voteFor=-1
							DPrintf("C to F, %d\n",rf.me)

						}else{
							if voteMe>len(rf.peers)/2{
								rf.role= Leader
								DPrintf("C to L, %d\n",rf.me)
								//init log
								rf.nextIndex= make([]int,len(rf.peers))
								rf.matchIndex= make([]int,len(rf.peers))
								for i,_:= range rf.nextIndex{
									rf.nextIndex[i]=rf.GetLastLogIndex()+1
								}
							}
						}
					}

			case Leader:
				rf.mu.Unlock()
			}
		}

	}
}

// func leader append calls
func (rf *Raft)leaderBeat(){
	for !rf.killed(){
		time.Sleep(50*time.Millisecond)
		rf.mu.Lock()
		if rf.role!=Leader{
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i,_:= range rf.peers{
			if i==rf.me{
				continue
			}
			rf.broadcast(i)
		}
	}

}
type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PrevLogIndex int
	PreLogTerm int
	Entries 	[]Log
	LeaderCommitIndex int
}
type AppendEntriesReply struct{
	Term int
	Success bool
}
func (rf *Raft)broadcast(id int){
	args:= AppendEntriesArgs{}
	rf.mu.Lock()
	args.Term=rf.currentTerm
	args.LeaderId= rf.me
	args.PrevLogIndex=rf.nextIndex[id]-1
	args.PreLogTerm=rf.logEntries[args.PrevLogIndex].Term
	args.LeaderCommitIndex= rf.commitIndex
	args.Entries=make([]Log,0)
	args.Entries=append(args.Entries,rf.logEntries[rf.nextIndex[id]:]...)
	rf.mu.Unlock()
	//send append rpc, no lock
	go func(){
		reply:= AppendEntriesReply{}
		ok:= rf.sendAppendEntries(id,&args,&reply)
		if ok{
			if reply.Success{
				rf.mu.Lock()
				rf.nextIndex[id] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[id] = rf.nextIndex[id] - 1
				rf.mu.Unlock()
			}else{
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm<reply.Term{
					DPrintf("L to F, %d",rf.me)
					rf.currentTerm=reply.Term
					rf.role=Follower
					rf.voteFor=-1
				}else{
					//same term just rebroadcast
					rf.nextIndex[id]--
				}

			}
		}
	}()

}
//
func (rf *Raft)appendHandle(){

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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm= 0
	rf.voteFor= -1
	rf.logEntries= make([]Log,0)
	rf.commitIndex=0
	rf.lastApplied= 0
	rf.role= Follower
	rf.loopStartTime= time.Now()
	// initialize from state persisted before a crash
	DPrintf("a node start,my id is %d\n",rf.me)
	//rf.readPersist(persister.ReadRaftState())

	// leader boardcast routine
	go rf.leaderBeat()

	// election routine
	go rf.election()
	// sync log
	go rf.sync()
	return rf
}
