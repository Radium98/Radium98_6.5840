package raft

import (
	"math/rand"
	"time"
)

// let the base election timeout be T.
// the election timeout is in the range [T, 2T).
const baseElectionTimeout = 300
const None = -1

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimer()
	rf.becomeCandidate()
	done := false
	votes := 1
	term := rf.currentTerm
	args := RequestVoteArgs{rf.currentTerm, rf.me}

	for i, _ := range rf.peers {
		if rf.me == i {
			continue
		}
		// 开启协程去尝试拉选票
		go func(serverId int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if !ok || !reply.VoteGranted {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm > reply.Term {
				return
			}
			// 统计票数
			votes++
			if done || votes <= len(rf.peers)/2 {
				// 在成为leader之前如果投票数不足需要继续收集选票
				// 同时在成为leader的那一刻，就不需要管剩余节点的响应了，因为已经具备成为leader的条件
				return
			}
			done = true
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			rf.state = Leader // 将自身设置为leader
			go rf.StartAppendEntries(true) // 立即发送心跳

		}(i)
	}
}

func (rf *Raft) pastElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f := time.Since(rf.lastElection) > rf.electionTimeout
	return f
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
	
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

}
func (rf *Raft) ToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	rf.votedFor = None
}

// example RequestVoteRPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//竞选leader的节点任期小于等于自己的任期，则反对票(为什么等于情况也反对票呢？因为candidate节点在发送requestVote rpc之前会将自己的term+1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = None
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	//Lab2B的日志复制直接确定为true
	update := true
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		//竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer() //自己的票已经投出时就转为follower状态
		reply.VoteGranted = true // 默认设置响应体为投同意票状态
	} else {
		reply.VoteGranted = false
	}
}

// example RequestVoteRPC handler.
func (rf *Raft) RequestVote2(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//竞选leader的节点任期小于等于自己的任期，则反对票(为什么等于情况也反对票呢？因为candidate节点在发送requestVote rpc之前会将自己的term+1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = None
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	//Lab2B的日志复制直接确定为true
	update := true
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		//竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer()  //自己的票已经投出时就转为follower状态
		reply.VoteGranted = true // 默认设置响应体为投同意票状态
	} else {
		reply.VoteGranted = false
	}

}