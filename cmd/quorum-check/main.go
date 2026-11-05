// quorum-check drives a running cluster with concurrent clients, records every request, and checks
// that the whole history is linearizable. Run it while you kill, restart and cut off nodes.
//
//	quorum-check -nodes 1=http://localhost:7001,2=http://localhost:7002,3=http://localhost:7003 -duration 60s
//
// Exit status 1 means the cluster returned something no correct store could have.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Lurosan237/Quorum/client"
	"github.com/Lurosan237/Quorum/linear"
)

func main() {
	nodesFlag := flag.String("nodes", "", "the cluster: id=url,id=url,...")
	clients := flag.Int("clients", 8, "concurrent clients")
	keys := flag.Int("keys", 5, "keys to spread the load over")
	duration := flag.Duration("duration", 30*time.Second, "how long to run")
	seed := flag.Int64("seed", time.Now().UnixNano()%1_000_000, "seed for the workload")
	out := flag.String("out", "", "write the history and result here as JSON")
	minOK := flag.Int("min-ok", 1, "fail if fewer requests than this were answered (the cluster must make progress)")
	flag.Parse()

	nodes, err := client.ParseNodes(*nodesFlag)
	if err != nil {
		log.Fatal(err)
	}
	t0 := time.Now()
	hist, st := client.Run(nodes, client.Workload{Clients: *clients, Keys: *keys, Duration: *duration, Seed: *seed})
	elapsed := time.Since(t0)
	res := linear.Check(hist, 0)
	fmt.Printf("%d requests in %s: %d answered (%.0f/s), %d unknown\n", st.Ops, elapsed.Round(time.Millisecond),
		st.OK, float64(st.OK)/elapsed.Seconds(), st.Unknown)
	fmt.Printf("latency p50 %s  p99 %s  max %s\n", st.Percentile(50).Round(time.Microsecond),
		st.Percentile(99).Round(time.Microsecond), st.Percentile(100).Round(time.Microsecond))
	if *out != "" {
		b, _ := json.MarshalIndent(map[string]any{"seed": *seed, "requests": st.Ops, "answered": st.OK,
			"unknown": st.Unknown, "seconds": elapsed.Seconds(), "p50_ms": ms(st.Percentile(50)),
			"p99_ms": ms(st.Percentile(99)), "linearizable": res.OK, "gave_up": res.GaveUp,
			"bad_key": res.BadKey, "explain": res.Explain, "history": hist}, "", " ")
		os.WriteFile(*out, b, 0o644)
	}
	switch {
	case res.GaveUp:
		fmt.Printf("INCONCLUSIVE: the search ran out of budget on a key (%d states)\n", res.Explored)
		os.Exit(2)
	case !res.OK:
		fmt.Printf("NOT LINEARIZABLE on key %s:\n%s\n", res.BadKey, res.Explain)
		os.Exit(1)
	case st.OK < *minOK:
		fmt.Printf("NO PROGRESS: only %d requests answered (want %d)\n", st.OK, *minOK)
		os.Exit(1)
	}
	fmt.Printf("linearizable: %d keys, %d operations, %d states explored\n", res.Keys, res.Ops, res.Explored)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
