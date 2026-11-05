package sim

import (
	"math"
	"sort"

	"github.com/Lurosan237/Quorum/kv"
	"github.com/Lurosan237/Quorum/linear"
	"github.com/Lurosan237/Quorum/raft"
)

// What the browser demo draws, and the levers it pulls. None of this is used by the tests' runs.

// NodeView is one server as the visualiser sees it.
type NodeView struct {
	ID        int               `json:"id"`
	Up        bool              `json:"up"`
	Role      string            `json:"role"`
	Term      uint64            `json:"term"`
	Vote      int               `json:"vote"`
	Lead      int               `json:"lead"`
	Commit    uint64            `json:"commit"`
	Applied   uint64            `json:"applied"`
	LastIndex uint64            `json:"last"`
	SnapIndex uint64            `json:"snap"`
	Tail      []EntryView       `json:"tail"` // the last entries of its log
	Data      map[string]string `json:"data"`
	Side      int               `json:"side"` // partition side (-1 = no partition)
}

// EntryView is one log entry.
type EntryView struct {
	Index uint64 `json:"i"`
	Term  uint64 `json:"t"`
	Op    string `json:"op"`
}

// MsgView is one message in flight.
type MsgView struct {
	From int    `json:"from"` // client messages use -1 - client index
	To   int    `json:"to"`
	Kind string `json:"kind"`
	Sent int64  `json:"sent"`
	At   int64  `json:"at"`
	Cut  bool   `json:"cut"` // will be lost to a partition
}

// View is the whole world at one tick.
type View struct {
	Now        int64      `json:"now"`
	Nodes      []NodeView `json:"nodes"`
	Msgs       []MsgView  `json:"msgs"`
	Events     []Event    `json:"events"` // since the last view
	Completed  int        `json:"completed"`
	Elections  int        `json:"elections"`
	Crashes    int        `json:"crashes"`
	Violations []string   `json:"violations"`
	Committed  uint64     `json:"committed"`
	Profile    string     `json:"profile"`
	Waiting    bool       `json:"waiting"` // the person's own request is in flight
}

// View draws the world; events are handed out once.
func (s *Sim) View(tail int) View {
	v := View{Now: s.now, Completed: s.res.Completed, Elections: s.res.Elections, Crashes: s.res.Crashes,
		Violations: s.res.Violations, Committed: s.res.Committed, Profile: s.cfg.Faults.Name, Events: s.Events,
		Msgs: []MsgView{}}
	s.Events = nil
	for _, srv := range s.servers {
		n := NodeView{ID: srv.id, Up: srv.up, Role: "down", Vote: -1, Lead: -1, Side: -1}
		if g, ok := s.group[srv.id]; ok {
			n.Side = g
		}
		if srv.up {
			st := srv.node.Status()
			n.Role, n.Term, n.Vote, n.Lead = st.Role.String(), st.Term, st.Vote, st.Lead
			n.Commit, n.Applied, n.LastIndex, n.SnapIndex = st.Commit, st.Applied, st.LastIndex, st.SnapIndex
			from := st.SnapIndex + 1
			if st.LastIndex >= uint64(tail) && st.LastIndex-uint64(tail)+1 > from {
				from = st.LastIndex - uint64(tail) + 1
			}
			for _, e := range srv.node.Log() {
				if e.Index < from {
					continue
				}
				ev := EntryView{Index: e.Index, Term: e.Term}
				if e.Data != nil {
					if op, err := kv.Decode(e.Data); err == nil {
						ev.Op = string(op.Kind) + " " + op.Key + " " + op.Value
					}
				}
				n.Tail = append(n.Tail, ev)
			}
			n.Data = srv.store.Data
		} else {
			hs := srv.saved.HardState
			n.Term, n.Vote, n.Commit = hs.Term, hs.Vote, hs.Commit
		}
		v.Nodes = append(v.Nodes, n)
	}
	for _, e := range s.net {
		m := MsgView{From: e.from, To: e.to, Sent: e.sent, At: e.at, Cut: s.cut(s.side(e.from), s.side(e.to))}
		switch {
		case e.msg != nil:
			m.Kind = e.msg.Type.String()
			if e.msg.Type == raft.MsgApp && len(e.msg.Entries) == 0 {
				m.Kind = "Heartbeat"
			}
		case e.req != nil:
			m.Kind = "Request"
		default:
			m.Kind = "Reply"
		}
		if e.from >= clientBase {
			m.From = -1 - (e.from - clientBase)
		}
		if e.to >= clientBase {
			m.To = -1 - (e.to - clientBase)
		}
		v.Msgs = append(v.Msgs, m)
	}
	for _, c := range s.clients {
		if c.manual && c.pending != nil {
			v.Waiting = true
		}
	}
	return v
}

// Crash cuts a server's power: it loses everything it had not saved.
func (s *Sim) Crash(id int) {
	srv := s.servers[id]
	if !srv.up {
		return
	}
	srv.up, srv.node, srv.readyRead = false, nil, nil
	s.res.Crashes++
	s.event("crash", id, "you cut the power to server %d", id)
}

// Restart brings a server back from what it saved.
func (s *Sim) Restart(id int) {
	srv := s.servers[id]
	if srv.up {
		return
	}
	for i, r := range s.restarts {
		if r.srv == srv {
			s.restarts = append(s.restarts[:i], s.restarts[i+1:]...)
			break
		}
	}
	s.boot(srv)
	s.event("restart", id, "server %d is back (from what it had saved)", id)
}

// Isolate cuts one server off from all the others until Heal.
func (s *Sim) Isolate(id int) {
	s.group = map[int]int{}
	for _, srv := range s.servers {
		s.group[srv.id] = 1
	}
	s.group[id] = 0
	s.healAt = math.MaxInt64
	s.res.Partitions++
	s.event("split", id, "you cut server %d off the network", id)
}

// Heal joins the network again.
func (s *Sim) Heal() {
	if s.group != nil {
		s.group = nil
		s.event("heal", -1, "the network is whole again")
	}
}

// SetFaults changes the weather from now on.
func (s *Sim) SetFaults(f Faults) {
	s.cfg.Faults = f
	s.nextCrash = s.after(f.CrashEvery)
	s.nextSplit = s.after(f.SplitEvery)
	s.res.Profile = f.Name
}

// Ask sends a request from the person at the keyboard. It returns false while their previous
// request is still on its way.
func (s *Sim) Ask(kind, key, value string) bool {
	var c *client
	for _, cl := range s.clients {
		if cl.manual {
			c = cl
		}
	}
	if c == nil {
		c = &client{id: uint64(len(s.clients) + 1), leader: -1, manual: true}
		s.clients = append(s.clients, c)
	}
	if c.pending != nil {
		return false
	}
	c.target = 0
	for _, srv := range s.servers {
		if srv.up && srv.node.Status().Role == raft.Leader {
			c.target = srv.id
		}
	}
	c.seq++
	op := kv.Op{Kind: kv.Kind(kind), Key: key, Value: value, Client: c.id, Seq: c.seq}
	c.pending, c.call = &op, s.now
	s.clientSend(c)
	return true
}

// Check runs the linearizability checker over everything answered so far (unanswered writes may
// or may not have happened), without ending the run.
func (s *Sim) Check() linear.Result {
	h := append([]linear.Op(nil), s.history...)
	for _, c := range s.clients {
		if c.pending != nil && c.pending.Kind != kv.Get {
			op := *c.pending
			h = append(h, linear.Op{Client: c.id, Kind: string(op.Kind), Key: op.Key, Value: op.Value, Call: c.call, Unknown: true})
		}
	}
	sort.SliceStable(h, func(i, j int) bool { return h[i].Call < h[j].Call })
	return linear.Check(h, 2_000_000)
}

// History is everything the clients were told, so far.
func (s *Sim) History() []linear.Op { return s.history }
