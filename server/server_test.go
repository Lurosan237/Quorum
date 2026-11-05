package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Lurosan237/Quorum/client"
	"github.com/Lurosan237/Quorum/linear"
	"github.com/Lurosan237/Quorum/raft"
)

func freePorts(t *testing.T, n int) []string {
	var addrs []string
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs = append(addrs, l.Addr().String())
		l.Close()
	}
	return addrs
}

type cluster struct {
	t       *testing.T
	cfg     map[int]Config
	servers map[int]*Server
}

func startCluster(t *testing.T, n int) *cluster {
	addrs := freePorts(t, n)
	peers := map[int]string{}
	for i, a := range addrs {
		peers[i+1] = "http://" + a
	}
	c := &cluster{t: t, cfg: map[int]Config{}, servers: map[int]*Server{}}
	dir := t.TempDir()
	for i, a := range addrs {
		id := i + 1
		c.cfg[id] = Config{ID: id, Peers: peers, Listen: a, DataDir: filepath.Join(dir, fmt.Sprint(id)),
			Tick: 5 * time.Millisecond, CompactEvery: 100, Logger: log.New(io.Discard, "", 0)}
		c.start(id)
	}
	t.Cleanup(func() {
		for _, s := range c.servers {
			s.Stop()
		}
	})
	return c
}

func (c *cluster) start(id int) {
	s, err := New(c.cfg[id])
	if err != nil {
		c.t.Fatal(err)
	}
	s.Start()
	c.servers[id] = s
}

func (c *cluster) stop(id int) {
	c.servers[id].Stop()
	delete(c.servers, id)
}

func (c *cluster) leader() int {
	for id, s := range c.servers {
		ch := make(chan raft.Status, 1)
		s.status <- ch
		if st := <-ch; st.Role == raft.Leader {
			return id
		}
	}
	return 0
}

func (c *cluster) peers() map[int]string { return c.cfg[1].Peers }

// Three real servers over HTTP, the leader and then a follower restarted from disk mid-run: every
// answer must still fit one sequential order.
func TestClusterSurvivesRestartsAndStaysLinearizable(t *testing.T) {
	c := startCluster(t, 3)
	done := make(chan struct{})
	var hist []linear.Op
	var st client.Stats
	go func() {
		hist, st = client.Run(c.peers(), client.Workload{Clients: 6, Keys: 4, Duration: 4 * time.Second, Seed: 1})
		close(done)
	}()
	for _, wait := range []time.Duration{800 * time.Millisecond, 1200 * time.Millisecond} {
		time.Sleep(wait)
		victim := c.leader()
		if victim == 0 {
			victim = 1
		}
		c.stop(victim)
		time.Sleep(300 * time.Millisecond)
		c.start(victim)
	}
	<-done
	res := linear.Check(hist, 0)
	t.Logf("%d requests, %d answered, %d unknown, p50 %s", st.Ops, st.OK, st.Unknown, st.Percentile(50))
	if !res.OK || res.GaveUp {
		t.Fatalf("not linearizable on %s:\n%s", res.BadKey, res.Explain)
	}
	if st.OK < 100 {
		t.Fatalf("only %d requests answered", st.OK)
	}
}

// A crash halfway through writing a record leaves a torn last line: everything before it stands.
func TestWALSurvivesATornWrite(t *testing.T) {
	dir := t.TempDir()
	w, _, err := openWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.save(raft.Ready{MustSave: true, HardState: raft.HardState{Term: 3, Vote: 2, Commit: 1},
		EntriesFrom: 1, Entries: []raft.Entry{{Index: 1, Term: 3}, {Index: 2, Term: 3, Data: []byte("x")}}})
	w.save(raft.Ready{EntriesFrom: 2, Entries: []raft.Entry{{Index: 2, Term: 4, Data: []byte("y")}}})
	w.f.Close()
	f, _ := os.OpenFile(filepath.Join(dir, "wal.jsonl"), os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString(`{"hs":{"Term":9,"Vo`)
	f.Close()

	_, p, err := openWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if p.HardState.Term != 3 || p.HardState.Vote != 2 {
		t.Fatalf("hard state %+v", p.HardState)
	}
	if len(p.Entries) != 2 || p.Entries[1].Term != 4 || string(p.Entries[1].Data) != "y" {
		t.Fatalf("entries %+v (the second save must replace index 2)", p.Entries)
	}
}

// After a snapshot the log is rewritten, and a restart finds the same state.
func TestWALRewriteKeepsState(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := openWAL(dir)
	p := raft.Persisted{HardState: raft.HardState{Term: 2, Vote: 1, Commit: 7},
		Snapshot: &raft.Snapshot{Index: 5, Term: 2, Data: []byte(`{}`)},
		Entries:  []raft.Entry{{Index: 6, Term: 2}, {Index: 7, Term: 2}}}
	if err := w.rewrite(p); err != nil {
		t.Fatal(err)
	}
	w.save(raft.Ready{EntriesFrom: 8, Entries: []raft.Entry{{Index: 8, Term: 2}}})
	w.f.Close()
	_, got, _ := openWAL(dir)
	if got.Snapshot == nil || got.Snapshot.Index != 5 || len(got.Entries) != 3 || got.HardState.Commit != 7 {
		t.Fatalf("got %+v", got)
	}
}
