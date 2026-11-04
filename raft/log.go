package raft

// raftLog holds the entries after the last snapshot. Index i lives at entries[i-snapIndex-1].
type raftLog struct {
	snapshot  *Snapshot
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry
}

func newLog(s *Snapshot, es []Entry) raftLog {
	l := raftLog{snapshot: s}
	if s != nil {
		l.snapIndex, l.snapTerm = s.Index, s.Term
	}
	l.entries = append([]Entry(nil), es...)
	return l
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.snapTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// term of the entry at i; false if i is beyond the log or compacted away (the snapshot's own index
// still answers, and index 0 is term 0).
func (l *raftLog) term(i uint64) (uint64, bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	}
	return l.entries[i-l.snapIndex-1].Term, true
}

// append writes entries, dropping anything from their first index on.
func (l *raftLog) append(es []Entry) {
	if len(es) == 0 {
		return
	}
	first := es[0].Index
	keep := first - l.snapIndex - 1
	l.entries = append(l.entries[:keep:keep], es...)
}

// slice returns up to n entries from index i.
func (l *raftLog) slice(i uint64, n int) []Entry {
	if i > l.lastIndex() {
		return nil
	}
	start := i - l.snapIndex - 1
	end := min(uint64(len(l.entries)), start+uint64(n))
	return append([]Entry(nil), l.entries[start:end]...)
}

// between returns entries lo..hi inclusive (lo after the snapshot).
func (l *raftLog) between(lo, hi uint64) []Entry {
	if lo > hi || hi > l.lastIndex() || lo <= l.snapIndex {
		return nil
	}
	return append([]Entry(nil), l.entries[lo-l.snapIndex-1:hi-l.snapIndex]...)
}

// lastIndexOfTerm is the last index holding term t, or 0.
func (l *raftLog) lastIndexOfTerm(t uint64) uint64 {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Term == t {
			return l.entries[i].Index
		}
		if l.entries[i].Term < t {
			break
		}
	}
	return 0
}

// compact drops entries up to the snapshot's index.
func (l *raftLog) compact(s *Snapshot) {
	drop := s.Index - l.snapIndex
	l.entries = append([]Entry(nil), l.entries[drop:]...)
	l.snapshot, l.snapIndex, l.snapTerm = s, s.Index, s.Term
}

// restore replaces the whole log with a snapshot from the leader.
func (l *raftLog) restore(s *Snapshot) {
	if t, ok := l.term(s.Index); ok && t == s.Term && s.Index <= l.lastIndex() {
		l.compact(s) // we already have the entries after it: keep them
		return
	}
	l.entries = nil
	l.snapshot, l.snapIndex, l.snapTerm = s, s.Index, s.Term
}
