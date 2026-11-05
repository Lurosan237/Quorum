// Package server runs a Raft node as a real process: a clock, an HTTP transport to the other
// servers, a write-ahead log on disk, and an HTTP API for clients.
//
// One goroutine owns the Raft node and the store (no locks around them): it ticks the clock, steps
// incoming messages, takes client requests, and processes Ready in the order Raft requires (save,
// then send, then apply). Sending happens on per-peer goroutines with bounded queues, so a slow or
// unreachable peer never blocks the others; a full queue drops messages, which Raft tolerates.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Lurosan237/Quorum/kv"
	"github.com/Lurosan237/Quorum/raft"
)

// Config of a server.
type Config struct {
	ID           int
	Peers        map[int]string // id -> base URL (http://host:port), including this server
	Listen       string         // address to serve on
	DataDir      string
	Tick         time.Duration // one Raft tick
	CompactEvery uint64
	Logger       *log.Logger
	NoBatch      bool // handle one request per disk write (benchmarks only: shows what batching buys)
	NoSync       bool // skip fsync (benchmarks only: unsafe, a power cut can lose acknowledged writes)
}

type proposal struct {
	op    kv.Op
	reply chan result
}

type read struct {
	key   string
	reply chan result
}

type result struct {
	Value  string
	Err    error
	Leader int
}

type waiter struct {
	term  uint64
	reply chan result
}

type readWait struct {
	key   string
	index uint64
	reply chan result
}

// Server is one running node.
type Server struct {
	cfg          Config
	node         *raft.Node
	store        *kv.Store
	wal          *wal
	inbox        chan raft.Message
	props        chan proposal
	reads        chan read
	status       chan chan raft.Status
	waiting      map[uint64]waiter
	pendingReads map[uint64]readWait
	readyReads   []readWait
	readSeq      uint64
	peers        map[int]chan raft.Message
	http         *http.Server
	client       *http.Client
	stop         chan struct{}
	wg           sync.WaitGroup
}

var (
	ErrNotLeader = errors.New("not the leader")
	ErrRetry     = errors.New("leadership changed before the request committed: retry")
)

// New opens the data directory and builds the node from what it finds.
func New(cfg Config) (*Server, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	w, saved, err := openWAL(cfg.DataDir)
	if w != nil {
		w.noSync = cfg.NoSync
	}
	if err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	node := raft.New(raft.Config{ID: cfg.ID, Peers: ids, ElectionTicks: 20, HeartbeatTicks: 3,
		Seed: time.Now().UnixNano() + int64(cfg.ID), PreVote: true, CheckQuorum: true}, saved)
	s := &Server{cfg: cfg, node: node, store: kv.New(), wal: w,
		inbox: make(chan raft.Message, 4096), props: make(chan proposal, 1024), reads: make(chan read, 1024),
		status: make(chan chan raft.Status), waiting: map[uint64]waiter{}, pendingReads: map[uint64]readWait{},
		peers: map[int]chan raft.Message{}, stop: make(chan struct{}),
		client: &http.Client{Timeout: 2 * time.Second}}
	for id := range cfg.Peers {
		if id != cfg.ID {
			s.peers[id] = make(chan raft.Message, 1024)
		}
	}
	return s, nil
}

// Start serves HTTP and runs the node until Stop.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft", s.handleRaft)
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handleWrite(kv.Put))
	mux.HandleFunc("POST /kv/{key}", s.handleWrite(kv.Append))
	mux.HandleFunc("GET /status", s.handleStatus)
	s.http = &http.Server{Addr: s.cfg.Listen, Handler: mux}
	for id, q := range s.peers {
		s.wg.Add(1)
		go s.sender(id, q)
	}
	s.wg.Add(1)
	go s.run()
	go func() {
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.cfg.Logger.Printf("http: %v", err)
		}
	}()
	return nil
}

// Stop shuts the server down (the data directory keeps everything saved).
func (s *Server) Stop() {
	close(s.stop)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.http.Shutdown(ctx)
	s.wg.Wait()
	s.wal.f.Close()
}

// ---- the node's own goroutine -------------------------------------------------------------------

func (s *Server) run() {
	defer s.wg.Done()
	tick := time.NewTicker(s.cfg.Tick)
	defer tick.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-tick.C:
			s.node.Tick()
		case m := <-s.inbox:
			s.node.Step(m)
		case p := <-s.props:
			s.propose(p)
		case r := <-s.reads:
			s.startRead(r)
		case c := <-s.status:
			c <- s.node.Status()
		}
		if !s.cfg.NoBatch {
			s.drain()
		}
		s.process()
	}
}

// drain takes everything else already waiting, so that one Ready (one disk write, one message
// per peer) covers many requests: under load this is what makes a leader fast.
func (s *Server) drain() {
	for i := 0; i < 512; i++ {
		select {
		case m := <-s.inbox:
			s.node.Step(m)
		case p := <-s.props:
			s.propose(p)
		case r := <-s.reads:
			s.startRead(r)
		default:
			return
		}
	}
}

func (s *Server) propose(p proposal) {
	idx, term, err := s.node.Propose(p.op.Encode())
	if err != nil {
		p.reply <- result{Err: ErrNotLeader, Leader: s.node.Status().Lead}
		return
	}
	s.waiting[idx] = waiter{term: term, reply: p.reply}
}

func (s *Server) startRead(r read) {
	if st := s.node.Status(); st.Role != raft.Leader { // reads go to the leader, like writes
		r.reply <- result{Err: ErrNotLeader, Leader: st.Lead}
		return
	}
	s.readSeq++
	if err := s.node.ReadIndex(s.readSeq); err != nil {
		r.reply <- result{Err: ErrNotLeader, Leader: s.node.Status().Lead}
		return
	}
	s.pendingReads[s.readSeq] = readWait{key: r.key, reply: r.reply}
}

func (s *Server) process() {
	for s.node.HasReady() {
		rd := s.node.Ready()
		if err := s.wal.save(rd); err != nil {
			s.cfg.Logger.Fatalf("wal: %v", err) // a node that cannot save must not go on
		}
		for _, m := range rd.Messages {
			select {
			case s.peers[m.To] <- m:
			default: // queue full: drop it, Raft retries
			}
		}
		if rd.ApplySnapshot && rd.Snapshot != nil {
			st, err := kv.Restore(rd.Snapshot.Data)
			if err != nil {
				s.cfg.Logger.Fatalf("restore snapshot: %v", err)
			}
			s.store = st
		}
		for _, e := range rd.CommittedEntries {
			var value string
			if e.Data != nil {
				op, err := kv.Decode(e.Data)
				if err == nil {
					value, _ = s.store.Apply(e.Index, op)
				}
			}
			if w, ok := s.waiting[e.Index]; ok {
				delete(s.waiting, e.Index)
				if w.term == e.Term {
					w.reply <- result{Value: value}
				} else {
					w.reply <- result{Err: ErrRetry}
				}
			}
		}
		for _, rs := range rd.ReadStates {
			if w, ok := s.pendingReads[rs.ID]; ok {
				delete(s.pendingReads, rs.ID)
				w.index = rs.Index
				s.readyReads = append(s.readyReads, w)
			}
		}
		s.node.Advance(rd)
	}
	st := s.node.Status()
	keep := s.readyReads[:0]
	for _, w := range s.readyReads {
		if st.Applied >= w.index {
			w.reply <- result{Value: s.store.Read(w.key)}
		} else {
			keep = append(keep, w)
		}
	}
	s.readyReads = keep
	if s.cfg.CompactEvery > 0 && st.Applied-st.SnapIndex >= s.cfg.CompactEvery {
		if err := s.node.Compact(st.Applied, s.store.Snapshot()); err == nil {
			s.process()
			if err := s.wal.rewrite(s.node.Persisted()); err != nil {
				s.cfg.Logger.Fatalf("wal rewrite: %v", err)
			}
		}
	}
	if st.Role != raft.Leader {
		for idx, w := range s.waiting {
			if idx > st.Commit {
				w.reply <- result{Err: ErrRetry, Leader: st.Lead}
				delete(s.waiting, idx)
			}
		}
		for id, w := range s.pendingReads { // a read confirmed by a lost leadership is no read at all
			w.reply <- result{Err: ErrRetry, Leader: st.Lead}
			delete(s.pendingReads, id)
		}
	}
}

// ---- transport ----------------------------------------------------------------------------------

// sender batches a peer's messages into HTTP posts.
func (s *Server) sender(id int, q chan raft.Message) {
	defer s.wg.Done()
	url := s.cfg.Peers[id] + "/raft"
	for {
		var batch []raft.Message
		select {
		case <-s.stop:
			return
		case m := <-q:
			batch = append(batch, m)
		}
	more:
		for len(batch) < 256 {
			select {
			case m := <-q:
				batch = append(batch, m)
			default:
				break more
			}
		}
		b, _ := json.Marshal(batch)
		resp, err := s.client.Post(url, "application/json", bytes.NewReader(b))
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}

func (s *Server) handleRaft(w http.ResponseWriter, r *http.Request) {
	var batch []raft.Message
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, m := range batch {
		select {
		case s.inbox <- m:
		default:
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- the client API -----------------------------------------------------------------------------
//
//	GET  /kv/{key}                        read (linearizable, via ReadIndex)
//	PUT  /kv/{key}   body = value         put
//	POST /kv/{key}   body = value         append
//
// Writes carry X-Client and X-Seq headers: a retry with the same pair is applied once.
// A server that is not the leader answers 421 with X-Leader: the leader's id, when known.

func (s *Server) answer(w http.ResponseWriter, res result) {
	switch {
	case res.Err == nil:
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, res.Value)
	case errors.Is(res.Err, ErrNotLeader):
		w.Header().Set("X-Leader", strconv.Itoa(res.Leader))
		http.Error(w, res.Err.Error(), http.StatusMisdirectedRequest)
	default:
		http.Error(w, res.Err.Error(), http.StatusServiceUnavailable)
	}
}

func (s *Server) wait(r *http.Request, c chan result) (result, bool) {
	select {
	case res := <-c:
		return res, true
	case <-r.Context().Done():
		return result{}, false
	case <-time.After(3 * time.Second):
		return result{Err: ErrRetry}, true
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	c := make(chan result, 1)
	s.reads <- read{key: r.PathValue("key"), reply: c}
	if res, ok := s.wait(r, c); ok {
		s.answer(w, res)
	}
}

func (s *Server) handleWrite(kind kv.Kind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		client, _ := strconv.ParseUint(r.Header.Get("X-Client"), 10, 64)
		seq, _ := strconv.ParseUint(r.Header.Get("X-Seq"), 10, 64)
		c := make(chan result, 1)
		s.props <- proposal{op: kv.Op{Kind: kind, Key: r.PathValue("key"), Value: string(body), Client: client, Seq: seq}, reply: c}
		if res, ok := s.wait(r, c); ok {
			s.answer(w, res)
		}
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	c := make(chan raft.Status, 1)
	select {
	case s.status <- c:
	case <-time.After(time.Second):
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}
	st := <-c
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"id": st.ID, "role": st.Role.String(), "term": st.Term, "leader": st.Lead,
		"commit": st.Commit, "applied": st.Applied, "last_index": st.LastIndex, "snapshot": st.SnapIndex})
}

// String for logs.
func (s *Server) String() string { return fmt.Sprintf("server %d", s.cfg.ID) }
