package raft

type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}
