// quorum-sim runs simulated clusters through failure scenarios and checks every run.
//
//	go run ./cmd/quorum-sim -profile everything -seeds 200
//	go run ./cmd/quorum-sim -profile flaky-mobile -seed 42 -v   # replay one run, with its events
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Lurosan237/Quorum/sim"
)

func main() {
	profile := flag.String("profile", "everything", "calm, flaky-mobile, power-cuts, split-network, everything, or all")
	seeds := flag.Int("seeds", 50, "how many seeded runs per profile")
	seed := flag.Int64("seed", 0, "run this one seed only")
	nodes := flag.Int("nodes", 5, "servers")
	clients := flag.Int("clients", 6, "clients")
	keys := flag.Int("keys", 4, "keys (fewer keys = more contention)")
	ticks := flag.Int("ticks", 4000, "length of a run in ticks (10 ms each)")
	readIndex := flag.Bool("readindex", false, "serve reads with ReadIndex instead of the log")
	compact := flag.Int("compact", 64, "snapshot every N applied entries (0 = never)")
	bug := flag.String("bug", "", "break something on purpose: no-sessions, forget-vote")
	verbose := flag.Bool("v", false, "print the events of each run")
	flag.Parse()

	names := []string{*profile}
	if *profile == "all" {
		names = nil
		for n := range sim.Profiles {
			names = append(names, n)
		}
		sort.Strings(names)
	}
	failed := false
	for _, name := range names {
		f, ok := sim.Profiles[name]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown profile %q\n", name)
			os.Exit(2)
		}
		list := []int64{}
		if *seed != 0 {
			list = append(list, *seed)
		} else {
			for i := 1; i <= *seeds; i++ {
				list = append(list, int64(i))
			}
		}
		start := time.Now()
		var ops, unknown, elections, crashes, splits int
		bad := []string{}
		for _, sd := range list {
			s := sim.New(sim.Config{Nodes: *nodes, Clients: *clients, Keys: *keys, Ticks: *ticks, Seed: sd,
				Faults: f, ReadIndex: *readIndex, CompactEvery: *compact, Bug: *bug})
			r := s.Run()
			ops += r.Completed
			unknown += r.Unknown
			elections += r.Elections
			crashes += r.Crashes
			splits += r.Partitions
			if *verbose {
				for _, e := range s.Events {
					fmt.Printf("  t=%5d  %-8s %s\n", e.Time, e.Kind, e.Text)
				}
			}
			if !r.OK() {
				failed = true
				why := strings.Join(r.Violations, "; ")
				if !r.Linear.OK {
					why += fmt.Sprintf(" not linearizable on key %s (gave up: %v)\n%s", r.Linear.BadKey, r.Linear.GaveUp, r.Linear.Explain)
				}
				bad = append(bad, fmt.Sprintf("seed %d: %s", sd, why))
			}
		}
		fmt.Printf("%-14s %4d runs  %7d ops answered  %4d unanswered  %5d elections  %4d crashes  %4d splits  %3d failed  (%.1fs)\n",
			name, len(list), ops, unknown, elections, crashes, splits, len(bad), time.Since(start).Seconds())
		for _, b := range bad {
			if len(b) > 1500 {
				b = b[:1500] + " ..."
			}
			fmt.Println("   ", b)
		}
	}
	if failed {
		os.Exit(1)
	}
}
