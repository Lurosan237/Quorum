// Package sim runs a whole Quorum cluster on simulated time: servers, a network, clients and the
// failures the network and the power grid throw at them, all driven by one seeded random source.
// A run is a pure function of its seed, so any failure found can be replayed exactly.
//
// Every message (between servers, and between clients and servers) goes through the simulated
// network, which can delay it, drop it, deliver it twice, or not deliver it across a partition.
// Servers crash (losing everything not yet saved) and restart from what they saved. Clients send
// one request at a time, retry when they get no answer, and record what they saw; the history is
// then checked for linearizability (package linear), and Raft's safety properties are checked on
// every step.
package sim

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/Lurosan237/Quorum/kv"
	"github.com/Lurosan237/Quorum/linear"
	"github.com/Lurosan237/Quorum/raft"
)

// Faults describes how hostile the world is. Times are in ticks (one tick = 10 ms of simulated time).
type Faults struct {
	Name       string
	Drop       float64 // share of messages lost
	Duplicate  float64 // share delivered twice
	DelayMin   int
	DelayMax   int
	CrashEvery int // mean ticks between server crashes (0 = never)
	DownMin    int // how long a crashed server stays down
	DownMax    int
	SplitEvery int // mean ticks between network partitions (0 = never)
	SplitMin   int
	SplitMax   int
}

// Profiles: the failures this store is built for.
var Profiles = map[string]Faults{
	"calm":         {Name: "calm", DelayMin: 1, DelayMax: 3},
	"flaky-mobile": {Name: "flaky-mobile", Drop: 0.10, Duplicate: 0.02, DelayMin: 1, DelayMax: 12},
	"power-cuts":   {Name: "power-cuts", DelayMin: 1, DelayMax: 3, CrashEvery: 150, DownMin: 30, DownMax: 300},
	// brief flickers, often in the middle of an election: what a vote that never reached the disk would break
	"brownouts":     {Name: "brownouts", DelayMin: 1, DelayMax: 4, CrashEvery: 25, DownMin: 1, DownMax: 15},
	"split-network": {Name: "split-network", DelayMin: 1, DelayMax: 3, SplitEvery: 250, SplitMin: 50, SplitMax: 400},
	"everything": {Name: "everything", Drop: 0.08, Duplicate: 0.02, DelayMin: 1, DelayMax: 10,
		CrashEvery: 200, DownMin: 30, DownMax: 300, SplitEvery: 300, SplitMin: 50, SplitMax: 400},
}

// Config of a run.
type Config struct {
	Nodes         int
	Clients       int
	Keys          int
	Ticks         int
	Seed          int64
	Faults        Faults
	ReadIndex     bool // serve gets with ReadIndex instead of through the log
	CompactEvery  int  // snapshot once this many entries are applied past the last one (0 = never)
	NoPreVote     bool
	NoCheckQuorum bool
	ClientTimeout int
	Bug           string // deliberately break something, to show the checks catch it (tests only)
}

// Event is something worth showing (the visualiser draws these).
type Event struct {
	Time int64  `json:"t"`
	Kind string `json:"kind"`
	Node int    `json:"node"`
	Text string `json:"text"`
}

// Result of a run.
type Result struct {
	Seed       int64
	Profile    string
	Completed  int // client ops answered
	Unknown    int // ops still waiting at the end (outcome unknown)
	Elections  int // leaders elected
	Crashes    int
	Partitions int
	Violations []string
	Linear     linear.Result
	LatencyP50 int64 // ticks from first send to answer
	LatencyP99 int64
	Committed  uint64
}

// OK says the run broke nothing.
func (r Result) OK() bool { return len(r.Violations) == 0 && r.Linear.OK }

// ---- the world -----------------------------------------------------------------------------------

const clientBase = 1000 // client ids on the network

type envelope struct {
	sent int64
	at   int64
	seq  int64
	from int
	to   int
	msg  *raft.Message
	req  *request
	resp *response
}

type request struct {
	Op kv.Op
}

type response struct {
	Seq      uint64
	OK       bool
	Value    string
	Redirect int // leader hint when !OK (-1 = unknown)
}

type waiter struct {
	term   uint64
	client int
	op     kv.Op
}

type server struct {
	id        int
	node      *raft.Node
	store     *kv.Store
	saved     raft.Persisted
	up        bool
	waiting   map[uint64]waiter   // log index -> who to answer
	reads     map[uint64]readWait // ReadIndex request id -> read
	readyRead []readWait
	readSeq   uint64
}

type readWait struct {
	client int
	op     kv.Op
	index  uint64
}

type client struct {
	id       uint64
	seq      uint64
	pending  *kv.Op
	call     int64
	sentAt   int64
	target   int
	leader   int
	idleTill int64
	manual   bool // driven by a person (the browser demo), not by the random workload
	answer   string
}

// Sim is one simulated world.
type Sim struct {
	cfg                  Config
	rng                  *rand.Rand
	now                  int64
	seq                  int64
	servers              []*server
	clients              []*client
	net                  []envelope
	group                map[int]int // partition: node -> side; nil = whole
	healAt               int64
	nextSplit, nextCrash int64
	restarts             []restart

	history []linear.Op
	latency []int64
	Events  []Event
	res     Result

	leaders map[uint64]int        // term -> the leader seen (election safety)
	applied map[uint64]raft.Entry // index -> entry applied by anyone (state machine safety)
}

// New builds a world from a config.
func New(cfg Config) *Sim {
	if cfg.ClientTimeout == 0 {
		cfg.ClientTimeout = 60
	}
	s := &Sim{cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed)), leaders: map[uint64]int{},
		applied: map[uint64]raft.Entry{}}
	s.res.Seed, s.res.Profile = cfg.Seed, cfg.Faults.Name
	for i := 0; i < cfg.Nodes; i++ {
		srv := &server{id: i, up: true}
		s.servers = append(s.servers, srv)
		s.boot(srv)
	}
	for c := 0; c < cfg.Clients; c++ {
		s.clients = append(s.clients, &client{id: uint64(c + 1), leader: -1, target: s.rng.Intn(cfg.Nodes)})
	}
	s.nextCrash = s.after(cfg.Faults.CrashEvery)
	s.nextSplit = s.after(cfg.Faults.SplitEvery)
	return s
}

func (s *Sim) after(mean int) int64 {
	if mean == 0 {
		return -1
	}
	return s.now + int64(mean/2+s.rng.Intn(mean))
}

func (s *Sim) peers() []int {
	ps := make([]int, s.cfg.Nodes)
	for i := range ps {
		ps[i] = i
	}
	return ps
}

// boot starts (or restarts) a server from what it saved.
func (s *Sim) boot(srv *server) {
	cfg := raft.Config{ID: srv.id, Peers: s.peers(), ElectionTicks: 20, HeartbeatTicks: 3,
		Seed: s.rng.Int63(), PreVote: !s.cfg.NoPreVote, CheckQuorum: !s.cfg.NoCheckQuorum}
	srv.node = raft.New(cfg, srv.saved)
	srv.store = kv.New()
	srv.waiting = map[uint64]waiter{}
	srv.reads = map[uint64]readWait{}
	srv.readyRead = nil
	srv.up = true
}

func (s *Sim) event(kind string, node int, format string, args ...any) {
	s.Events = append(s.Events, Event{Time: s.now, Kind: kind, Node: node, Text: fmt.Sprintf(format, args...)})
}

// ---- the network ---------------------------------------------------------------------------------

func (s *Sim) cut(a, b int) bool {
	if s.group == nil {
		return false
	}
	ga, okA := s.group[a]
	gb, okB := s.group[b]
	return okA && okB && ga != gb
}

func (s *Sim) post(e envelope) {
	f := s.cfg.Faults
	if s.rng.Float64() < f.Drop {
		return
	}
	copies := 1
	if s.rng.Float64() < f.Duplicate {
		copies = 2
	}
	for i := 0; i < copies; i++ {
		e.sent = s.now
		e.at = s.now + int64(f.DelayMin+s.rng.Intn(f.DelayMax-f.DelayMin+1))
		s.seq++
		e.seq = s.seq
		s.net = append(s.net, e)
	}
}

// endpoint of a client: it reaches the network through the node it is closest to (its target),
// so a partition cuts a client off from the other side too.
func (s *Sim) side(endpoint int) int {
	if endpoint >= clientBase {
		return s.clients[endpoint-clientBase].target
	}
	return endpoint
}

func (s *Sim) deliver() {
	sort.SliceStable(s.net, func(i, j int) bool {
		if s.net[i].at != s.net[j].at {
			return s.net[i].at < s.net[j].at
		}
		return s.net[i].seq < s.net[j].seq
	})
	cut := 0
	for cut < len(s.net) && s.net[cut].at <= s.now {
		cut++
	}
	due := s.net[:cut]
	s.net = append([]envelope(nil), s.net[cut:]...)
	for _, e := range due {
		if s.cut(s.side(e.from), s.side(e.to)) {
			continue
		}
		switch {
		case e.resp != nil:
			s.clientReceive(s.clients[e.to-clientBase], e.resp)
		default:
			srv := s.servers[e.to]
			if !srv.up {
				continue
			}
			if e.msg != nil {
				srv.node.Step(*e.msg)
			} else {
				s.serve(srv, e.from, e.req.Op)
			}
		}
	}
}

// ---- servers -------------------------------------------------------------------------------------

func (s *Sim) reply(srv *server, to int, r response) {
	s.post(envelope{from: srv.id, to: to, resp: &r})
}

func (s *Sim) serve(srv *server, from int, op kv.Op) {
	st := srv.node.Status()
	if st.Role != raft.Leader {
		s.reply(srv, from, response{Seq: op.Seq, Redirect: st.Lead})
		return
	}
	if op.Kind == kv.Get && s.cfg.ReadIndex {
		srv.readSeq++
		srv.reads[srv.readSeq] = readWait{client: from, op: op}
		_ = srv.node.ReadIndex(srv.readSeq)
		return
	}
	data := op.Encode()
	if s.cfg.Bug == "no-sessions" {
		op.Client = 0
		data = op.Encode()
	}
	idx, term, err := srv.node.Propose(data)
	if err != nil {
		s.reply(srv, from, response{Seq: op.Seq, Redirect: -1})
		return
	}
	srv.waiting[idx] = waiter{term: term, client: from, op: op}
}

// process does everything a server's Ready asks, until it has nothing more to do.
func (s *Sim) process(srv *server) {
	for i := 0; i < 8 && srv.node.HasReady(); i++ {
		rd := srv.node.Ready()
		// 1. save (the simulator's disk is the node's own view once Advance has run, below)
		// 2. send
		for _, m := range rd.Messages {
			m := m
			s.post(envelope{from: srv.id, to: m.To, msg: &m})
		}
		// 3. apply
		if rd.ApplySnapshot && rd.Snapshot != nil {
			st, _ := kv.Restore(rd.Snapshot.Data)
			srv.store = st
		}
		for _, e := range rd.CommittedEntries {
			s.checkApplied(srv, e)
			if e.Data == nil {
				continue
			}
			op, err := kv.Decode(e.Data)
			if err != nil {
				s.violate("server %d applied an undecodable entry at %d", srv.id, e.Index)
				continue
			}
			if s.cfg.Bug == "no-sessions" {
				op.Client = 0
			}
			result, _ := srv.store.Apply(e.Index, op)
			if w, ok := srv.waiting[e.Index]; ok {
				delete(srv.waiting, e.Index)
				if w.term == e.Term {
					s.reply(srv, w.client, response{Seq: w.op.Seq, OK: true, Value: result})
				} else {
					s.reply(srv, w.client, response{Seq: w.op.Seq, Redirect: -1}) // its entry was overwritten
				}
			}
		}
		for _, r := range rd.ReadStates {
			if w, ok := srv.reads[r.ID]; ok {
				delete(srv.reads, r.ID)
				w.index = r.Index
				srv.readyRead = append(srv.readyRead, w)
			}
		}
		srv.node.Advance(rd)
		srv.saved = srv.node.Persisted()
		if s.cfg.Bug == "forget-vote" {
			srv.saved.HardState.Vote = -1 // pretends the vote never reached the disk
		}
	}
	// reads whose index is now applied
	keep := srv.readyRead[:0]
	for _, w := range srv.readyRead {
		if srv.node.Status().Applied >= w.index {
			s.reply(srv, w.client, response{Seq: w.op.Seq, OK: true, Value: srv.store.Read(w.op.Key)})
		} else {
			keep = append(keep, w)
		}
	}
	srv.readyRead = keep
	// compaction
	st := srv.node.Status()
	if s.cfg.CompactEvery > 0 && st.Applied-st.SnapIndex >= uint64(s.cfg.CompactEvery) {
		_ = srv.node.Compact(st.Applied, srv.store.Snapshot())
		s.process(srv)
	}
	// a leader that lost its job answers nobody: tell its clients to try elsewhere
	if st.Role != raft.Leader && len(srv.waiting) > 0 {
		idxs := make([]uint64, 0, len(srv.waiting))
		for idx := range srv.waiting {
			idxs = append(idxs, idx) // in order: map order is random, and a run must replay exactly
		}
		sort.Slice(idxs, func(i, j int) bool { return idxs[i] < idxs[j] })
		for _, idx := range idxs {
			w := srv.waiting[idx]
			if idx > st.Commit {
				s.reply(srv, w.client, response{Seq: w.op.Seq, Redirect: st.Lead})
				delete(srv.waiting, idx)
			}
		}
	}
}

// ---- safety checks on every step ------------------------------------------------------------------

func (s *Sim) violate(format string, args ...any) {
	msg := fmt.Sprintf("t=%d: ", s.now) + fmt.Sprintf(format, args...)
	if len(s.res.Violations) < 20 {
		s.res.Violations = append(s.res.Violations, msg)
	}
}

func (s *Sim) checkApplied(srv *server, e raft.Entry) {
	if prev, ok := s.applied[e.Index]; ok {
		if prev.Term != e.Term || string(prev.Data) != string(e.Data) {
			s.violate("state machine safety: server %d applied a different entry at index %d (term %d vs %d)",
				srv.id, e.Index, e.Term, prev.Term)
		}
		return
	}
	s.applied[e.Index] = e
	if e.Index > s.res.Committed {
		s.res.Committed = e.Index
	}
}

func (s *Sim) checkLeaders() {
	for _, srv := range s.servers {
		if !srv.up {
			continue
		}
		st := srv.node.Status()
		if st.Role != raft.Leader {
			continue
		}
		if l, ok := s.leaders[st.Term]; !ok {
			s.leaders[st.Term] = srv.id
			s.res.Elections++
			s.event("leader", srv.id, "server %d is leader for term %d", srv.id, st.Term)
		} else if l != srv.id {
			s.violate("election safety: servers %d and %d both lead term %d", l, srv.id, st.Term)
		}
	}
}

// ---- clients -------------------------------------------------------------------------------------

func (s *Sim) clientAct(c *client) {
	if c.pending == nil {
		if c.manual || s.now < c.idleTill {
			return
		}
		c.seq++
		op := kv.Op{Client: c.id, Seq: c.seq, Key: fmt.Sprintf("k%d", s.rng.Intn(max(1, s.cfg.Keys)))}
		switch r := s.rng.Float64(); {
		case r < 0.4:
			op.Kind = kv.Get
		case r < 0.65:
			op.Kind, op.Value = kv.Put, fmt.Sprintf("%d.%d ", c.id, c.seq)
		default:
			op.Kind, op.Value = kv.Append, fmt.Sprintf("%d.%d ", c.id, c.seq)
		}
		c.pending, c.call = &op, s.now
		if c.leader >= 0 {
			c.target = c.leader
		}
		s.clientSend(c)
		return
	}
	if s.now-c.sentAt >= int64(s.cfg.ClientTimeout) {
		c.leader = -1
		c.target = (c.target + 1 + s.rng.Intn(s.cfg.Nodes-1)) % s.cfg.Nodes // try someone else
		s.clientSend(c)
	}
}

func (s *Sim) clientSend(c *client) {
	c.sentAt = s.now
	s.post(envelope{from: clientBase + int(c.id) - 1, to: c.target, req: &request{Op: *c.pending}})
}

func (s *Sim) clientReceive(c *client, r *response) {
	if c.pending == nil || r.Seq != c.pending.Seq {
		return // an answer to an earlier attempt we already settled
	}
	if !r.OK {
		if r.Redirect >= 0 && r.Redirect != c.target {
			c.leader, c.target = r.Redirect, r.Redirect
		} else {
			c.target = (c.target + 1) % s.cfg.Nodes
		}
		s.clientSend(c)
		return
	}
	op := *c.pending
	s.history = append(s.history, linear.Op{Client: c.id, Kind: string(op.Kind), Key: op.Key, Value: op.Value,
		Output: r.Value, Call: c.call, Return: s.now})
	s.latency = append(s.latency, s.now-c.call)
	s.res.Completed++
	if c.manual {
		s.event("you", c.target, "your %s %s answered by server %d: %q", op.Kind, op.Key, c.target, r.Value)
	}
	c.leader = c.target
	c.pending = nil
	c.idleTill = s.now + int64(s.rng.Intn(5))
}

// ---- faults --------------------------------------------------------------------------------------

func (s *Sim) faults() {
	f := s.cfg.Faults
	if s.nextCrash >= 0 && s.now >= s.nextCrash {
		s.nextCrash = s.after(f.CrashEvery)
		// crash someone at random, but keep a majority able to come back
		srv := s.servers[s.rng.Intn(len(s.servers))]
		if srv.up {
			srv.up = false
			srv.node = nil
			downFor := int64(f.DownMin + s.rng.Intn(f.DownMax-f.DownMin+1))
			srv.readyRead = nil
			s.res.Crashes++
			s.event("crash", srv.id, "power cut: server %d is down", srv.id)
			s.schedule(downFor, srv)
		}
	}
	keep := s.restarts[:0]
	for _, r := range s.restarts {
		if s.now >= r.at {
			s.boot(r.srv)
			s.event("restart", r.srv.id, "server %d is back (from what it had saved)", r.srv.id)
		} else {
			keep = append(keep, r)
		}
	}
	s.restarts = keep
	if s.group != nil && s.now >= s.healAt {
		s.group = nil
		s.event("heal", -1, "the network is whole again")
	}
	if s.nextSplit >= 0 && s.now >= s.nextSplit && s.group == nil {
		s.nextSplit = s.after(f.SplitEvery)
		s.group = map[int]int{}
		order := s.rng.Perm(s.cfg.Nodes)
		k := 1 + s.rng.Intn(s.cfg.Nodes-1)
		var a, b []int
		for i, n := range order {
			if i < k {
				s.group[n] = 0
				a = append(a, n)
			} else {
				s.group[n] = 1
				b = append(b, n)
			}
		}
		s.healAt = s.now + int64(f.SplitMin+s.rng.Intn(f.SplitMax-f.SplitMin+1))
		s.res.Partitions++
		s.event("split", -1, "the network splits: %v | %v", a, b)
	}
}

type restart struct {
	at  int64
	srv *server
}

func (s *Sim) schedule(after int64, srv *server) {
	s.restarts = append(s.restarts, restart{at: s.now + after, srv: srv})
}

// ---- running -------------------------------------------------------------------------------------

// Step advances the world by one tick.
func (s *Sim) Step() {
	s.now++
	s.deliver()
	for _, srv := range s.servers {
		if srv.up {
			srv.node.Tick()
		}
	}
	for _, srv := range s.servers {
		if srv.up {
			s.process(srv)
		}
	}
	s.checkLeaders()
	for _, c := range s.clients {
		s.clientAct(c)
	}
	s.faults()
}

// Run runs the whole configured duration, then checks the history.
func (s *Sim) Run() Result {
	for i := 0; i < s.cfg.Ticks; i++ {
		s.Step()
	}
	return s.Finish()
}

// Finish records unanswered ops as unknown and checks the history.
func (s *Sim) Finish() Result {
	h := append([]linear.Op(nil), s.history...)
	for _, c := range s.clients {
		if c.pending != nil {
			op := *c.pending
			if op.Kind != kv.Get { // an unanswered read changes nothing: leave it out
				h = append(h, linear.Op{Client: c.id, Kind: string(op.Kind), Key: op.Key, Value: op.Value,
					Call: c.call, Unknown: true})
			}
			s.res.Unknown++
		}
	}
	s.res.Linear = linear.Check(h, 0)
	if len(s.latency) > 0 {
		l := append([]int64(nil), s.latency...)
		sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
		s.res.LatencyP50 = l[len(l)/2]
		s.res.LatencyP99 = l[len(l)*99/100]
	}
	return s.res
}

// Now is the simulated time in ticks.
func (s *Sim) Now() int64 { return s.now }
