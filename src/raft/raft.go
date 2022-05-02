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
	"6.824/src/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)


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
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	LEADER 		int = 1
	FOLLOWER 	int = 2
	CANDIDATE 	int = 3
	DEAD		int = 4

	TLeader	 	time.Duration = time.Millisecond * 100
	TFollower	time.Duration = time.Millisecond * 400
	TCandidate	time.Duration = time.Millisecond * 1000
	TDead		time.Duration = time.Millisecond * 900  * 10
	)

type Entry struct {
	Command string
	Term    int
	me 		int
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

	state       		int
	CurrentTerm 		int
	VoteFor				int
	Log[] 				Entry
	nextIndex[] 		int

	HeartBeat			*time.Timer
	Candidate			*time.Timer
	heartTimeout 		bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//fmt.Printf("[GetState],id is %d,leader is %d\n",rf.me,rf.VoteFor)
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term,isleader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term 			int
	LeaderId		int
	PrevLogIndex[] 	int
	PrevLogTerm[]   int
	Logs[]			Entry
	LeaderCommit	int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 	int
	Leader	int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	/*	收到投票的几种情况
		1.赞成票
	      有且仅由rf.CurrentTerm < args.Term
		2.反对票
	      rf.CurrentTerm >= args.Term
	*/
	if args.Term > rf.CurrentTerm { //正常流程,新的选期大于当前选期
		rf.mu.Lock()
		rf.VoteFor     = args.LeaderId // 投票给请求的leader
		rf.CurrentTerm = args.Term
		rf.state	   = FOLLOWER
		rf.mu.Unlock()
		reply.Success  = true
		reply.Term     = rf.CurrentTerm
		reply.Leader   = rf.VoteFor
	} else { //反之则是新的candidate落后于整体
		reply.Success  = false
		reply.Term	   = rf.CurrentTerm
		reply.Leader   = rf.VoteFor
	}
	// Your code here (2A, 2B).
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

func (rf *Raft) sendKeepAlive(server int,args *KeepAliveRequest, reply *KeepAliveReply) bool {
	ok := rf.peers[server].Call("Raft.KeepAlive", args, reply)
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false { //假如peer挂掉了
		// TODO
		/*	1.reboot
			2.注册加入cluster成为follower
			3.更新本地的log entries
		 */
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(rf.RandomTimeout(DEAD))
	}
}

func (rf *Raft) LeaderKeepAliveTicker() {
	/*
		有且仅由自身是leader的情况下才会进行keepAlive的信息发送
	 */
	for {
		term,isLeader := rf.GetState() // isLeader一写多读
		rf.mu.Lock()
		args := &KeepAliveRequest { // 多读
			Term: term,
			Leader: rf.VoteFor,
		}
		rf.mu.Unlock()
		if isLeader {
			fmt.Printf("[Leader] KeepAlive start work ! id is %d,current is %d\n",rf.me,rf.CurrentTerm)
			for i := 0; i < len(rf.peers); i++ { // 不可以直接使用i，这里i是变量
				index := i
				go func() {
					reply := &KeepAliveReply{} // 多写
					if isLeader {
						if rf.sendKeepAlive(index,args,reply) { // RPC正常
							if !reply.Success {
								/*
									有且仅由当前节点落后时，才会回复false
								 */
								isLeader = false
								rf.mu.Lock()
								rf.state       = FOLLOWER
								rf.CurrentTerm = reply.Term
								rf.VoteFor     = reply.Leader // 新的leader
								rf.mu.Unlock()
								//rf.updateLogEntries()
							}
						} else { //RPC超时，可能是follower故障或者网络不通

						}
					}
				}()
			}
		}
		time.Sleep(rf.RandomTimeout(LEADER))
	}
}

func (rf *Raft) CandidateElectionTicker() {
	for {
		time.Sleep(rf.RandomTimeout(CANDIDATE))
		if rf.heartTimeout == true && rf.state == FOLLOWER {
			rf.Election() // 超时触发选举
		}
		rf.mu.Lock()
		rf.heartTimeout = true
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



	// Your initialization code here (2A, 2B, 2C).
	rf.state 		= FOLLOWER
	rf.heartTimeout = true
	rf.CurrentTerm  = 0
	rf.VoteFor      = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CandidateElectionTicker()
	go rf.LeaderKeepAliveTicker()
	//time.Sleep(rf.RandomTimeout(CANDIDATE))// maybe
	return rf
}


func (rf *Raft) Election() {
	/*
		1.更新自身状态为candidate
		2.投票给自己
		3.轮询的投票给所有server
		4.票数过半则自身状态更新为leader
	 */
	rf.mu.Lock()
	rf.state   = CANDIDATE
	rf.VoteFor = rf.me
	rf.CurrentTerm ++
	var count int32 = 0
	args  := &RequestVoteArgs { // 多读
		LeaderId: rf.me,
		Term: rf.CurrentTerm,
	}
	rf.mu.Unlock()
	allowToVote := true // 多读一写
	fmt.Printf("[candidate] : id is %d ,CurrentTerm is %d \n",rf.me,rf.CurrentTerm)
	for i := 0; i < len(rf.peers); i++ { // 不可以直接使用i，这里i是变量
		index := i
		if allowToVote {
			go func() {
				reply := &RequestVoteReply{}            // 多写
				if rf.sendRequestVote(index, args, reply) { // 如果RPC没有超时或出错
					if reply.Success {
						atomic.AddInt32(&count, 1) // 将count原子的+1
						fmt.Printf("[candidate] : count num is %d\n", count)
						if count*2+1 >= int32(len(rf.peers)) { // 票数过半
							rf.mu.Lock()
							if rf.state == CANDIDATE {
								rf.state = LEADER
							}
							rf.mu.Unlock()
							fmt.Printf("[Leader] : id is %d,current is %d\n",rf.me,rf.CurrentTerm)
						}
					} else { // 拉票失败
						/*
							1.当前candidate落后于整体进度
							2.当前选期已经投票给其他的candidate了
						*/
						if reply.Term > rf.CurrentTerm { // 选期落后
							/*
								1. 将自身状态由candidate或leader改为follower
								2. 向reply中提到的leader更新log entry
								3. 停止RPC拉票
							*/
							rf.mu.Lock()
							rf.state = FOLLOWER
							//rf.updateLogEntries()
							rf.mu.Unlock()
							allowToVote = false
						}
						if reply.Term == rf.CurrentTerm { // 已经投票给其他candidate
							fmt.Printf("[cnadidate] : %d ,Vote to other candidate\n",rf.me)
						}
					}
				} else {
					fmt.Printf("[cnadidate] : %d ,RPC timeout!\n",rf.me)
				}
			}()
		}
	}
}


type KeepAliveRequest struct {
	//2A
	Term 	int
	Leader  int
}

type KeepAliveReply struct {
	//2A
	Term    int
	Leader  int
	Success bool
}

func (rf *Raft) KeepAlive(args *KeepAliveRequest,reply *KeepAliveReply) {
	// TODO : 自身状态一定要确保为follower
	//if rf.state == LEADER { // 如果自身为candidate OR leader
	//	// to do sth
	//	return
	//}
	rf.mu.Lock()
	if args.Term > rf.CurrentTerm { // 代表当前follower落后于当前进度，此时需要更新当前follower的进度和log entry
		rf.VoteFor     = args.Leader
		rf.CurrentTerm = args.Term
		reply.Leader   = rf.VoteFor
		reply.Term     = rf.CurrentTerm // 需要自行更新log entry
		rf.state 	   = FOLLOWER
		// rf.updateLogEntry()
		reply.Success = true
		rf.heartTimeout = false // 更新超时状态
	}
	if args.Term == rf.CurrentTerm { // 代表进度正常
		if args.Leader != rf.VoteFor { // 如果出现了两个leader。。。理论不可能
			reply.Success = false
		} else {
			reply.Success = true
			reply.Term    = rf.CurrentTerm
			reply.Leader  = rf.VoteFor
		}
		rf.heartTimeout = false // 更新超时状态
	}
	if args.Term < rf.CurrentTerm { // 代表当前leader落后于整体进度，需要下台并且更新log entry
		reply.Term    = rf.CurrentTerm
		reply.Leader  = rf.VoteFor
		reply.Success = false
	}
	rf.mu.Unlock()
}


func (rf *Raft) RandomTimeout(Type int) (res time.Duration) {
	switch Type {
	case LEADER:
		res = TLeader    + time.Duration(rand.Int63()) % TLeader
	case FOLLOWER:
		res = TFollower  + time.Duration(rand.Int63()) % TFollower
	case CANDIDATE:
		res = TCandidate + time.Duration(rand.Int63()) % TCandidate
	case DEAD:
		res = TDead      + time.Duration(rand.Int63()) % TDead
	}
	// fmt.Printf("rf id is %d ,Type is %d,time is %s\n",rf.me,Type,res.String())
	return res
}