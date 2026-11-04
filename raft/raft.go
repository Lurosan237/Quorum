// Package raft is the Raft consensus algorithm as a pure state machine.
//
// A Node never touches a clock, a network or a disk. Time passes when the caller calls Tick;
// messages arrive when the caller calls Step; and everything the node wants done (messages to
// send, state to save, entries to apply) comes out of Ready. That makes the same code run in a
// deterministic simulator (thousands of seeded failure scenarios a second, every bug replayable),
// in real servers, and in the browser through WebAssembly.
//
// Implemented: leader election with randomised timeouts, PreVote (a cut-off node cannot force
// elections when it comes back), CheckQuorum (a leader that loses touch with a majority steps
// down), log replication with fast conflict backtracking, commit only of current-term entries
// (Raft paper section 5.4.2), snapshots for log compaction, and ReadIndex for linearizable reads
// without writing to the log.
package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
)

// Role of a node.
type Role int

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[r]
}

// Entry is one log entry. Data is nil for the no-op a new leader appends.
type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

// MsgType says what a message is.
type MsgType int

const (
	MsgPreVote MsgType = iota
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp         // append entries (also the heartbeat)
	MsgAppResp
	MsgSnap        // install snapshot
	MsgReadIndex   // follower asks the leader to confirm a read
	MsgReadIndexResp
)

var msgNames = [...]string{"PreVote", "PreVoteResp", "Vote", "VoteResp", "App", "AppResp", "Snap", "ReadIndex", "ReadIndexResp"}

func (t MsgType) String() string { return msgNames[t] }

// Message is everything nodes say to each other.
type Message struct {
	Type    MsgType
	From    int
	To      int
	Term    uint64
	LogTerm uint64 // vote: term of the candidate's last entry; append: term of the entry before Entries
	Index   uint64 // vote: candidate's last index; append: index before Entries; resp: match or hint
	Entries []Entry
	Commit  uint64
	Reject  bool
	// Fast backtracking: on a rejected append, the follower's conflicting term and its first index.
	HintTerm  uint64
	HintIndex uint64
	Snapshot  *Snapshot
	Context   uint64 // ReadIndex: the request id, echoed back
}

// Snapshot is the state machine at an index, plus the index and term it covers.
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// HardState is what must be on disk before any message from the same Ready is sent.
type HardState struct {
	Term   uint64
	Vote   int // -1 = none
	Commit uint64
}

// ReadState answers a ReadIndex request: once the node has applied up to Index, a read that
// started before the request may be served from the local state machine.
type ReadState struct {
	ID    uint64
	Index uint64
}

// Ready is what the caller must do, in this order: save HardState, Entries and Snapshot; send
// Messages; apply Snapshot then CommittedEntries; then call Advance.
type Ready struct {
	HardState        HardState
	MustSave         bool
	Entries          []Entry // to write to stable storage, replacing everything from EntriesFrom on
	EntriesFrom      uint64
	Snapshot         *Snapshot // to save (and, if ApplySnapshot, to restore the state machine from)
	ApplySnapshot    bool
	Messages         []Message
	CommittedEntries []Entry
	ReadStates       []ReadState
}

// Config of a node.
type Config struct {
	ID            int
	Peers         []int // all members, including ID
	ElectionTicks int   // a follower starts an election after [ElectionTicks, 2*ElectionTicks) ticks of silence
	HeartbeatTicks int
	MaxEntriesPerMsg int
	Seed          int64 // randomised election timeouts, seeded so simulations repeat exactly
	PreVote       bool
	CheckQuorum   bool
}

// Persisted is what a node finds on disk when it restarts.
type Persisted struct {
	HardState HardState
	Snapshot  *Snapshot
	Entries   []Entry // after the snapshot
}

var ErrNotLeader = errors.New("raft: not the leader")

// Node is one Raft participant.
type Node struct {
	cfg  Config
	rng  *rand.Rand
	role Role
	term uint64
	vote int
	lead int

	log      raftLog
	commit   uint64
	applied  uint64 // handed out for applying (via Ready)

	votes      map[int]bool
	next       map[int]uint64
	match      map[int]uint64
	active     map[int]bool // heard from since the last CheckQuorum
	probing    map[int]bool // waiting for a reply before sending more (one append in flight while probing)
	pendingSnap map[int]bool

	electionElapsed  int
	heartbeatElapsed int
	timeout          int

	// ReadIndex: requests waiting for the leader's commit of its own term, and for a heartbeat round.
	readSeq     uint64
	pendingRead []pendingRead
	readStates  []ReadState

	msgs        []Message
	dirtyFrom   uint64 // lowest index written since the last Ready (0 = none)
	snapToApply *Snapshot
	snapToSave  *Snapshot
	prevHS      HardState
}

type pendingRead struct {
	id     uint64
	from   int // node that asked (self for local reads)
	index  uint64
	acks   map[int]bool
}

// New creates a node from what it finds on disk (zero Persisted for a fresh one).
func New(cfg Config, p Persisted) *Node {
	if cfg.MaxEntriesPerMsg == 0 {
		cfg.MaxEntriesPerMsg = 64
	}
	n := &Node{cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed)), vote: -1, lead: -1}
	n.log = newLog(p.Snapshot, p.Entries)
	n.term, n.vote = p.HardState.Term, p.HardState.Vote
	if p.HardState.Term == 0 && p.HardState.Vote == 0 {
		n.vote = -1
	}
	n.commit = max(p.HardState.Commit, n.log.snapIndex)
	n.applied = n.log.snapIndex
	if p.Snapshot != nil {
		// the state machine restarts from the snapshot: hand it out once
		n.snapToApply = p.Snapshot
	}
	n.prevHS = n.hardState()
	n.becomeFollower(n.term, -1)
	return n
}

// ---- state transitions --------------------------------------------------------------------------

func (n *Node) resetTimeout() {
	n.electionElapsed = 0
	n.timeout = n.cfg.ElectionTicks + n.rng.Intn(n.cfg.ElectionTicks)
}

func (n *Node) becomeFollower(term uint64, lead int) {
	if term > n.term {
		n.term, n.vote = term, -1
	}
	n.role, n.lead = Follower, lead
	n.resetTimeout()
	n.pendingRead = nil
}

func (n *Node) becomePreCandidate() {
	n.role, n.lead = PreCandidate, -1
	n.votes = map[int]bool{n.cfg.ID: true}
	n.resetTimeout()
}

func (n *Node) becomeCandidate() {
	n.role, n.lead = Candidate, -1
	n.term++
	n.vote = n.cfg.ID
	n.votes = map[int]bool{n.cfg.ID: true}
	n.resetTimeout()
}

func (n *Node) becomeLeader() {
	n.role, n.lead = Leader, n.cfg.ID
	n.heartbeatElapsed = 0
	n.electionElapsed = 0
	n.next, n.match = map[int]uint64{}, map[int]uint64{}
	n.active, n.probing, n.pendingSnap = map[int]bool{}, map[int]bool{}, map[int]bool{}
	for _, p := range n.cfg.Peers {
		n.next[p] = n.log.lastIndex() + 1
		n.probing[p] = true
	}
	// A no-op in the new term: entries of earlier terms commit only once one of the leader's own does.
	n.appendEntries([]Entry{{Term: n.term, Index: n.log.lastIndex() + 1}})
	n.match[n.cfg.ID] = n.log.lastIndex()
	n.broadcastAppend()
	n.maybeCommit()
}

// ---- time ---------------------------------------------------------------------------------------

// Tick advances the node's clock by one unit.
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		n.electionElapsed++
		if n.electionElapsed >= n.cfg.ElectionTicks {
			n.electionElapsed = 0
			if n.cfg.CheckQuorum && !n.quorumActive() {
				n.becomeFollower(n.term, -1) // cut off from a majority: stop serving
				return
			}
			n.active = map[int]bool{}
		}
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastHeartbeat()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.timeout {
		n.campaign()
	}
}

func (n *Node) quorumActive() bool {
	count := 1
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID && n.active[p] {
			count++
		}
	}
	return count >= n.quorum()
}

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) campaign() {
	if n.cfg.PreVote {
		n.becomePreCandidate()
		n.requestVotes(MsgPreVote, n.term+1)
	} else {
		n.becomeCandidate()
		n.requestVotes(MsgVote, n.term)
	}
	if len(n.cfg.Peers) == 1 {
		n.tallied(true)
	}
}

func (n *Node) requestVotes(t MsgType, term uint64) {
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.send(Message{Type: t, To: p, Term: term, Index: n.log.lastIndex(), LogTerm: n.log.lastTerm()})
		}
	}
}

// ---- messages -----------------------------------------------------------------------------------

// Step hands the node one message.
func (n *Node) Step(m Message) {
	switch {
	case m.Term == 0:
		// local or term-less: handled below
	case m.Type == MsgPreVote:
		// A pre-vote never changes our term. With CheckQuorum, ignore it while we have a live leader.
	case m.Type == MsgPreVoteResp && !m.Reject:
		// a granted pre-vote carries the future term; it does not mean someone is ahead of us
	case m.Term > n.term:
		if m.Type == MsgVote && n.cfg.CheckQuorum && n.lead != -1 && n.electionElapsed < n.cfg.ElectionTicks {
			return // we heard from a leader recently: don't let a disruptive candidate depose it
		}
		if m.Type == MsgApp || m.Type == MsgSnap {
			n.becomeFollower(m.Term, m.From)
		} else {
			n.becomeFollower(m.Term, -1)
		}
	case m.Term < n.term:
		if m.Type == MsgApp || m.Type == MsgSnap {
			// tell a stale leader about the newer term so it steps down
			n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true, Index: m.Index})
		}
		return
	}

	switch m.Type {
	case MsgPreVote, MsgVote:
		n.handleVote(m)
		return
	}

	switch n.role {
	case Leader:
		n.stepLeader(m)
	case Candidate, PreCandidate:
		n.stepCandidate(m)
	default:
		n.stepFollower(m)
	}
}

func (n *Node) handleVote(m Message) {
	upToDate := m.LogTerm > n.log.lastTerm() || (m.LogTerm == n.log.lastTerm() && m.Index >= n.log.lastIndex())
	if m.Type == MsgPreVote {
		// grant if the candidate's term would be ahead, its log is up to date, and we are not
		// hearing from a leader ourselves
		heardLeader := n.lead != -1 && n.electionElapsed < n.cfg.ElectionTicks
		grant := m.Term > n.term && upToDate && !heardLeader
		// A grant echoes the future term asked for; a refusal carries our own term, so a candidate
		// only steps down for someone genuinely ahead of it (echoing its future term back on a
		// refusal made terms climb with no election ever won)
		term := m.Term
		if !grant {
			term = n.term
		}
		n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: term, Reject: !grant})
		return
	}
	grant := (n.vote == -1 || n.vote == m.From) && upToDate && m.Term == n.term
	if grant {
		n.vote = m.From
		n.electionElapsed = 0
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: !grant})
}

func (n *Node) stepCandidate(m Message) {
	switch m.Type {
	case MsgApp, MsgSnap:
		n.becomeFollower(m.Term, m.From)
		n.stepFollower(m)
	case MsgPreVoteResp:
		if n.role != PreCandidate {
			return
		}
		n.votes[m.From] = !m.Reject
		n.tally()
	case MsgVoteResp:
		if n.role != Candidate {
			return
		}
		n.votes[m.From] = !m.Reject
		n.tally()
	}
}

func (n *Node) tally() {
	yes, no := 0, 0
	for _, v := range n.votes {
		if v {
			yes++
		} else {
			no++
		}
	}
	if yes >= n.quorum() {
		n.tallied(true)
	} else if no >= n.quorum() {
		n.becomeFollower(n.term, -1)
	}
}

func (n *Node) tallied(won bool) {
	if !won {
		return
	}
	if n.role == PreCandidate {
		n.becomeCandidate()
		n.requestVotes(MsgVote, n.term)
		if len(n.cfg.Peers) == 1 {
			n.becomeLeader()
		}
		return
	}
	n.becomeLeader()
}

func (n *Node) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		n.electionElapsed = 0
		n.lead = m.From
		n.handleAppend(m)
	case MsgSnap:
		n.electionElapsed = 0
		n.lead = m.From
		n.handleSnapshot(m)
	case MsgReadIndexResp:
		n.readStates = append(n.readStates, ReadState{ID: m.Context, Index: m.Index})
	}
}

func (n *Node) handleAppend(m Message) {
	if m.Index < n.log.snapIndex {
		// already compacted: everything up to the snapshot is committed, ack what we have
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Index: n.log.snapIndex})
		return
	}
	if t, ok := n.log.term(m.Index); !ok || t != m.LogTerm {
		// conflict: tell the leader where our log for that term starts, so it can skip back fast
		hintIndex := min(m.Index, n.log.lastIndex()+1)
		hintTerm, _ := n.log.term(hintIndex)
		if ok {
			for hintIndex > n.log.snapIndex+1 {
				if prev, _ := n.log.term(hintIndex - 1); prev != hintTerm {
					break
				}
				hintIndex--
			}
		}
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true, Index: m.Index,
			HintTerm: hintTerm, HintIndex: hintIndex})
		return
	}
	// skip entries we already have; truncate from the first conflict
	var fresh []Entry
	for i, e := range m.Entries {
		if t, ok := n.log.term(e.Index); !ok || t != e.Term {
			fresh = m.Entries[i:]
			break
		}
	}
	if len(fresh) > 0 {
		n.appendEntries(fresh)
	}
	last := m.Index + uint64(len(m.Entries))
	if m.Commit > n.commit {
		n.commit = min(m.Commit, last)
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Index: last})
}

func (n *Node) handleSnapshot(m Message) {
	s := m.Snapshot
	if s.Index <= n.commit {
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Index: n.commit})
		return
	}
	n.log.restore(s)
	n.commit = s.Index
	n.applied = s.Index
	n.snapToApply, n.snapToSave = s, s
	n.dirtyFrom = 0
	n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Index: s.Index})
}

func (n *Node) stepLeader(m Message) {
	switch m.Type {
	case MsgAppResp:
		n.active[m.From] = true
		n.ackReads(m.From)
		if m.Reject {
			if m.Term > n.term {
				return
			}
			// back off using the follower's hint: jump to the first index of its conflicting term
			next := m.HintIndex
			if m.HintTerm > 0 {
				if last := n.log.lastIndexOfTerm(m.HintTerm); last > 0 {
					next = last + 1
				}
			}
			n.next[m.From] = max(1, min(next, n.next[m.From]-1, m.Index))
			n.probing[m.From] = true
			n.sendAppend(m.From)
			return
		}
		if m.Index > n.match[m.From] {
			n.match[m.From] = m.Index
		}
		n.next[m.From] = max(n.next[m.From], m.Index+1)
		n.probing[m.From] = false
		delete(n.pendingSnap, m.From)
		if n.maybeCommit() {
			n.broadcastAppend()
		} else if n.next[m.From] <= n.log.lastIndex() {
			n.sendAppend(m.From)
		}
	case MsgReadIndex:
		n.startRead(m.Context, m.From)
	}
}

// ---- the leader's log work ----------------------------------------------------------------------

// Propose appends a command to the log. Only the leader accepts it; the caller learns it was
// applied when an entry with this index and term comes out of CommittedEntries.
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	e := Entry{Term: n.term, Index: n.log.lastIndex() + 1, Data: data}
	n.appendEntries([]Entry{e})
	n.match[n.cfg.ID] = e.Index
	n.broadcastAppend()
	n.maybeCommit()
	return e.Index, e.Term, nil
}

func (n *Node) appendEntries(es []Entry) {
	n.log.append(es)
	if n.dirtyFrom == 0 || es[0].Index < n.dirtyFrom {
		n.dirtyFrom = es[0].Index // storage rewrites the log from here on
	}
}

func (n *Node) broadcastAppend() {
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.sendAppend(p)
		}
	}
}

func (n *Node) broadcastHeartbeat() {
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.sendAppendForce(p)
		}
	}
}

// sendAppend sends what the follower lacks; while probing (after a rejection or as a new leader)
// it waits for a reply before sending more, so a lagging follower is not flooded.
func (n *Node) sendAppend(to int) {
	if n.probing[to] && n.pendingSnap[to] {
		return
	}
	if n.next[to] > n.log.lastIndex() && !n.probing[to] {
		return
	}
	n.sendAppendForce(to)
}

func (n *Node) sendAppendForce(to int) {
	prev := n.next[to] - 1
	prevTerm, ok := n.log.term(prev)
	if !ok {
		// the entries it needs are compacted away: send the snapshot instead
		s := n.log.snapshot
		n.send(Message{Type: MsgSnap, To: to, Term: n.term, Snapshot: s, Index: s.Index, LogTerm: s.Term})
		n.pendingSnap[to] = true
		n.probing[to] = true
		return
	}
	es := n.log.slice(prev+1, n.cfg.MaxEntriesPerMsg)
	if n.probing[to] && len(es) > 0 {
		es = es[:min(len(es), n.cfg.MaxEntriesPerMsg)]
	}
	n.send(Message{Type: MsgApp, To: to, Term: n.term, Index: prev, LogTerm: prevTerm, Entries: es,
		Commit: min(n.commit, prev+uint64(len(es)))})
	if !n.probing[to] && len(es) > 0 {
		n.next[to] = es[len(es)-1].Index + 1 // optimistic: keep sending while replication works
	}
}

// maybeCommit moves the commit index to the highest index a majority holds, if that entry is
// from the current term (an older term's entry is never committed by counting replicas).
func (n *Node) maybeCommit() bool {
	ms := make([]uint64, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		ms = append(ms, n.match[p])
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i] > ms[j] })
	idx := ms[n.quorum()-1]
	if idx > n.commit {
		if t, _ := n.log.term(idx); t == n.term {
			n.commit = idx
			n.servePendingReads()
			return true
		}
	}
	return false
}

// ---- linearizable reads (ReadIndex) -------------------------------------------------------------

// ReadIndex asks for a point in the log after which a read is linearizable. The answer comes back
// as a ReadState (in Ready) with the same id; serve the read once that index is applied.
func (n *Node) ReadIndex(id uint64) error {
	switch {
	case n.role == Leader:
		n.startRead(id, n.cfg.ID)
	case n.lead != -1:
		n.send(Message{Type: MsgReadIndex, To: n.lead, Term: n.term, Context: id})
	default:
		return ErrNotLeader
	}
	return nil
}

func (n *Node) startRead(id uint64, from int) {
	// The leader must have committed an entry of its own term (its no-op) before it knows the
	// true commit index; until then, park the read.
	pr := pendingRead{id: id, from: from, index: n.commit, acks: map[int]bool{n.cfg.ID: true}}
	n.pendingRead = append(n.pendingRead, pr)
	if len(n.cfg.Peers) == 1 {
		n.servePendingReads()
		return
	}
	n.broadcastHeartbeat() // a round of acks proves we are still the leader
}

func (n *Node) ackReads(from int) {
	for i := range n.pendingRead {
		n.pendingRead[i].acks[from] = true
	}
	n.servePendingReads()
}

func (n *Node) servePendingReads() {
	committedOwnTerm := false
	if t, _ := n.log.term(n.commit); t == n.term {
		committedOwnTerm = true
	}
	if !committedOwnTerm {
		return
	}
	keep := n.pendingRead[:0]
	for _, r := range n.pendingRead {
		if len(r.acks) < n.quorum() {
			keep = append(keep, r)
			continue
		}
		idx := max(r.index, n.commit)
		if r.from == n.cfg.ID {
			n.readStates = append(n.readStates, ReadState{ID: r.id, Index: idx})
		} else {
			n.send(Message{Type: MsgReadIndexResp, To: r.from, Term: n.term, Context: r.id, Index: idx})
		}
	}
	n.pendingRead = keep
}

// ---- output -------------------------------------------------------------------------------------

func (n *Node) send(m Message) {
	m.From = n.cfg.ID
	n.msgs = append(n.msgs, m)
}

func (n *Node) hardState() HardState { return HardState{Term: n.term, Vote: n.vote, Commit: n.commit} }

// HasReady says whether Ready has anything to do.
func (n *Node) HasReady() bool {
	return len(n.msgs) > 0 || n.dirtyFrom > 0 || n.commit > n.applied || n.snapToApply != nil ||
		n.snapToSave != nil || len(n.readStates) > 0 || n.hardState() != n.prevHS
}

// Ready collects the work to do. Call Advance after doing it.
func (n *Node) Ready() Ready {
	rd := Ready{HardState: n.hardState(), Messages: n.msgs, Snapshot: n.snapToApply, ReadStates: n.readStates}
	if n.snapToSave != nil {
		rd.Snapshot = n.snapToSave
	}
	if n.dirtyFrom > 0 {
		rd.Entries = n.log.between(n.dirtyFrom, n.log.lastIndex())
		rd.EntriesFrom = n.dirtyFrom
	}
	rd.ApplySnapshot = n.snapToApply != nil
	rd.MustSave = rd.HardState != n.prevHS || n.dirtyFrom > 0 || n.snapToSave != nil
	if n.commit > n.applied {
		rd.CommittedEntries = n.log.between(n.applied+1, n.commit)
	}
	return rd
}

// Advance marks a Ready as done.
func (n *Node) Advance(rd Ready) {
	n.msgs = nil
	n.dirtyFrom = 0
	n.readStates = nil
	n.snapToApply, n.snapToSave = nil, nil
	n.prevHS = rd.HardState
	if len(rd.CommittedEntries) > 0 {
		n.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
}

// Compact replaces the log up to index (already applied) with a snapshot of the state machine.
func (n *Node) Compact(index uint64, data []byte) error {
	if index > n.applied || index <= n.log.snapIndex {
		return fmt.Errorf("raft: cannot compact to %d (applied %d, snapshot %d)", index, n.applied, n.log.snapIndex)
	}
	t, _ := n.log.term(index)
	s := &Snapshot{Index: index, Term: t, Data: data}
	n.log.compact(s)
	n.snapToSave = s
	return nil
}

// ---- inspection (for tests, the simulator and the visualiser) -----------------------------------

type Status struct {
	ID        int
	Role      Role
	Term      uint64
	Vote      int
	Lead      int
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	SnapIndex uint64
	Match     map[int]uint64
}

func (n *Node) Status() Status {
	s := Status{ID: n.cfg.ID, Role: n.role, Term: n.term, Vote: n.vote, Lead: n.lead, Commit: n.commit,
		Applied: n.applied, LastIndex: n.log.lastIndex(), SnapIndex: n.log.snapIndex}
	if n.role == Leader {
		s.Match = map[int]uint64{}
		for k, v := range n.match {
			s.Match[k] = v
		}
	}
	return s
}

// Log returns the entries the node holds after its snapshot (for invariant checks).
func (n *Node) Log() []Entry { return n.log.between(n.log.snapIndex+1, n.log.lastIndex()) }

// TermAt returns the term of the entry at index, if the node has it (or it is the snapshot's).
func (n *Node) TermAt(index uint64) (uint64, bool) { return n.log.term(index) }

// Persisted is what the node has saved so far: what a restart would find on disk. (Real servers
// write Ready.Entries / HardState / Snapshot to a log file; the simulator just keeps this copy.)
func (n *Node) Persisted() Persisted {
	return Persisted{HardState: n.prevHS, Snapshot: n.log.snapshot,
		Entries: n.log.between(n.log.snapIndex+1, n.log.lastIndex())}
}
