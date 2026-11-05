package sim

import "testing"

// The demo's levers: cut the leader's power, cut a server off, send a request by hand, and the
// answers must still check out.
func TestDemoControls(t *testing.T) {
	s := New(Config{Nodes: 5, Clients: 4, Keys: 3, Seed: 3, Faults: Profiles["calm"], ReadIndex: true, CompactEvery: 24})
	leader := func() int {
		for _, n := range s.View(0).Nodes {
			if n.Role == "leader" {
				return n.ID
			}
		}
		return -1
	}
	for i := 0; i < 200; i++ {
		s.Step()
	}
	first := leader()
	if first < 0 {
		t.Fatal("no leader after 2 s")
	}
	s.Crash(first)
	s.Isolate((first + 1) % 5)
	for i := 0; i < 300; i++ {
		s.Step()
	}
	if l := leader(); l < 0 || l == first || l == (first+1)%5 {
		t.Fatalf("leader after the cut: %d", l)
	}
	if !s.Ask("append", "k0", "mine ") {
		t.Fatal("Ask refused")
	}
	for i := 0; i < 200 && s.View(0).Waiting; i++ {
		s.Step()
	}
	if s.View(0).Waiting {
		t.Fatal("the hand-sent request never got an answer")
	}
	s.Restart(first)
	s.Heal()
	for i := 0; i < 300; i++ {
		s.Step()
	}
	v := s.View(16)
	var top uint64
	for _, n := range v.Nodes {
		top = max(top, n.Commit)
	}
	for _, n := range v.Nodes {
		if !n.Up || n.Commit+5 < top {
			t.Fatalf("server %d did not catch up: %+v", n.ID, n)
		}
	}
	if r := s.Check(); !r.OK || len(v.Violations) > 0 {
		t.Fatalf("checks failed: %+v %v", r, v.Violations)
	}
}
