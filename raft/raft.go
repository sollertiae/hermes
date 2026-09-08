package raft

import (
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type CommitMsg struct {
	isValid    bool
	command    []byte
	commandIdx int
}

type Node struct {
	mu        sync.Mutex
	id        string
	peers     []string
	transport RPCTransport
	applyCh   chan<- CommitMsg

	// persistent state
	currentTerm int
	votedFor    string
	log         []LogEntry

	// shared in all nodes
	role        role
	commitIndex int
	lastApplied int

	// leader node
	nextIndex  map[string]int
	matchIndex map[string]int

	electionDeadline time.Time
	stopCh           chan struct{}
}

func NewNode(id string, peers []string, transport RPCTransport, applyCh chan<- commitMsg) *Node {
	n := &node{
		id:        id,
		peers:     peers,
		transport: transport,
		applyCh:   applyCh,
		role:      Follower,
		stopCh:    make(chan struct{}),
	}
	go n.run()
	return n
}

func (n *Node) Start(command []byte) (index int, term int, isLeader bool) {

}

func (n *Node) GetState() (term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == Leader
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) run() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	role := n.role
	timedOut := time.Now().After(n.electionDeadline)
	n.mu.Unlock()

	switch role {
	case Follower, Candidate:
		if timedOut {
			n.startElection()
		}
	case Leader:
		n.sendHeartbeats()
	}
}
