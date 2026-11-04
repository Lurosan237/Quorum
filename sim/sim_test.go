package sim

import (
	"reflect"
	"testing"
)

func run(profile string, seed int64, bug string, readIndex bool) Result {
	return New(Config{Nodes: 5, Clients: 6, Keys: 4, Ticks: 3000, Seed: seed, Faults: Profiles[profile],
		CompactEvery: 64, Bug: bug, ReadIndex: readIndex}).Run()
}

// Every failure scenario, many seeds: Raft's safety properties hold on every step and every
// client history is linearizable.
func TestEveryScenarioStaysCorrect(t *testing.T) {
	seeds := int64(40)
	if testing.Short() {
		seeds = 8
	}
	for name := range Profiles {
		for seed := int64(1); seed <= seeds; seed++ {
			for _, ri := range []bool{false, true} {
				if r := run(name, seed, "", ri); !r.OK() {
					t.Fatalf("%s seed %d readindex=%v: %v %+v", name, seed, ri, r.Violations, r.Linear)
				}
			}
		}
	}
}

// The checks are only worth something if they catch real bugs: break the store on purpose.
func TestTheChecksCatchDeliberateBugs(t *testing.T) {
	// without client sessions, a retried append is applied twice: not linearizable
	caught := 0
	for seed := int64(1); seed <= 10; seed++ {
		if r := run("flaky-mobile", seed, "no-sessions", false); !r.Linear.OK {
			caught++
		}
	}
	if caught == 0 {
		t.Fatal("double-applied appends went unnoticed")
	}
	// a vote that never reached the disk lets a flickering server vote twice in one term: two leaders
	if r := run("brownouts", 238, "forget-vote", false); len(r.Violations) == 0 {
		t.Fatal("two leaders in one term went unnoticed (seed 238 of brownouts finds them)")
	}
}

// A run is a pure function of its seed: any failure can be replayed exactly.
func TestRunsReplayExactly(t *testing.T) {
	a := New(Config{Nodes: 5, Clients: 6, Keys: 4, Ticks: 2000, Seed: 7, Faults: Profiles["everything"], CompactEvery: 64})
	b := New(Config{Nodes: 5, Clients: 6, Keys: 4, Ticks: 2000, Seed: 7, Faults: Profiles["everything"], CompactEvery: 64})
	ra, rb := a.Run(), b.Run()
	if !reflect.DeepEqual(ra, rb) || !reflect.DeepEqual(a.Events, b.Events) {
		t.Fatal("same seed, different run")
	}
}
