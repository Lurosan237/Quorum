// quorum-bench measures real Quorum servers (separate processes on this machine, talking over
// HTTP, with their logs on disk) and the simulator, and writes every number to one JSON file.
// bench/report.py turns that file into bench/REPORT.md and its charts; nothing in the report is
// typed by hand.
//
//	go build -o bin/quorum-server ./cmd/quorum-server
//	go run ./cmd/quorum-bench -server bin/quorum-server -out bench/results.json
//
// Every run of real servers is also checked for linearizability: a fast wrong answer is no result.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/Lurosan237/Quorum/client"
	"github.com/Lurosan237/Quorum/linear"
	"github.com/Lurosan237/Quorum/sim"
)

var (
	serverBin = flag.String("server", "bin/quorum-server", "the quorum-server binary")
	out       = flag.String("out", "bench/results.json", "where to write the results")
	runFor    = flag.Duration("run", 8*time.Second, "length of each load run")
	quick     = flag.Bool("quick", false, "a short version of everything (to try the harness)")
)

// ---- a cluster of real processes ------------------------------------------------------------------

type cluster struct {
	n     int
	dir   string
	base  int
	procs map[int]*exec.Cmd
	nodes map[int]string
	extra []string
}

var nextBase = 7400

func newCluster(n int, extra ...string) *cluster {
	dir, _ := os.MkdirTemp("", "quorum-bench-")
	c := &cluster{n: n, dir: dir, base: nextBase, procs: map[int]*exec.Cmd{}, nodes: map[int]string{}, extra: extra}
	nextBase += 10
	for i := 1; i <= n; i++ {
		c.nodes[i] = fmt.Sprintf("http://127.0.0.1:%d", c.base+i)
	}
	for i := 1; i <= n; i++ {
		c.start(i)
	}
	c.waitLeader(10 * time.Second)
	return c
}

func (c *cluster) peers() string {
	var p []string
	for i := 1; i <= c.n; i++ {
		p = append(p, fmt.Sprintf("%d=%s", i, c.nodes[i]))
	}
	return strings.Join(p, ",")
}

func (c *cluster) start(i int) {
	args := append([]string{"-id", fmt.Sprint(i), "-peers", c.peers(), "-listen", fmt.Sprintf("127.0.0.1:%d", c.base+i),
		"-data", filepath.Join(c.dir, fmt.Sprint(i))}, c.extra...)
	cmd := exec.Command(*serverBin, args...)
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	c.procs[i] = cmd
}

func (c *cluster) kill(i int) {
	if p := c.procs[i]; p != nil {
		p.Process.Signal(syscall.SIGKILL)
		p.Wait()
		delete(c.procs, i)
	}
}

func (c *cluster) stop() {
	for i := range c.procs {
		c.kill(i)
	}
	os.RemoveAll(c.dir)
}

type status struct {
	ID      int    `json:"id"`
	Role    string `json:"role"`
	Leader  int    `json:"leader"`
	Commit  uint64 `json:"commit"`
	Applied uint64 `json:"applied"`
	Snap    uint64 `json:"snapshot"`
}

var hc = &http.Client{Timeout: 300 * time.Millisecond}

func (c *cluster) status(i int) (status, bool) {
	var s status
	resp, err := hc.Get(c.nodes[i] + "/status")
	if err != nil {
		return s, false
	}
	defer resp.Body.Close()
	return s, json.NewDecoder(resp.Body).Decode(&s) == nil
}

func (c *cluster) leader() int {
	for i := range c.procs {
		if s, ok := c.status(i); ok && s.Role == "leader" {
			return i
		}
	}
	return 0
}

func (c *cluster) waitLeader(limit time.Duration) int {
	for t0 := time.Now(); time.Since(t0) < limit; time.Sleep(20 * time.Millisecond) {
		if l := c.leader(); l != 0 {
			return l
		}
	}
	log.Fatal("no leader")
	return 0
}

// ---- results ----------------------------------------------------------------------------------------

type loadResult struct {
	Name         string  `json:"name"`
	Nodes        int     `json:"nodes"`
	Clients      int     `json:"clients"`
	Mix          string  `json:"mix"`
	Batching     bool    `json:"batching"`
	Fsync        bool    `json:"fsync"`
	Seconds      float64 `json:"seconds"`
	Answered     int     `json:"answered"`
	Unknown      int     `json:"unknown"`
	OpsPerSec    float64 `json:"ops_per_sec"`
	P50ms        float64 `json:"p50_ms"`
	P90ms        float64 `json:"p90_ms"`
	P99ms        float64 `json:"p99_ms"`
	Linearizable bool    `json:"linearizable"`
}

type results struct {
	Machine    map[string]any   `json:"machine"`
	Scaling    []loadResult     `json:"scaling"`
	Mix        []loadResult     `json:"mix"`
	Ablation   []loadResult     `json:"ablation"`
	Failover   map[string]any   `json:"failover"`
	Catchup    []map[string]any `json:"catchup"`
	Simulation []map[string]any `json:"simulation"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func check(h []linear.Op) bool {
	r := linear.Check(h, 0)
	if !r.OK || r.GaveUp {
		log.Printf("NOT LINEARIZABLE (or inconclusive) on %s:\n%s", r.BadKey, r.Explain)
		return false
	}
	return true
}

func load(name string, nodes, clients int, mix string, batching, fsync bool) loadResult {
	var extra []string
	if !batching {
		extra = append(extra, "-no-batch")
	}
	if !fsync {
		extra = append(extra, "-no-fsync")
	}
	c := newCluster(nodes, extra...)
	defer c.stop()
	time.Sleep(300 * time.Millisecond)
	t0 := time.Now()
	h, st := client.Run(c.nodes, client.Workload{Clients: clients, Keys: 10, Duration: *runFor, Seed: int64(clients), Mix: mix})
	secs := time.Since(t0).Seconds()
	r := loadResult{Name: name, Nodes: nodes, Clients: clients, Mix: mix, Batching: batching, Fsync: fsync, Seconds: secs,
		Answered: st.OK, Unknown: st.Unknown, OpsPerSec: float64(st.OK) / secs, P50ms: ms(st.Percentile(50)),
		P90ms: ms(st.Percentile(90)), P99ms: ms(st.Percentile(99)), Linearizable: check(h)}
	log.Printf("%-10s %d nodes %3d clients %-6s batch=%v fsync=%v: %7.0f ops/s  p50 %6.2f ms  p99 %7.2f ms  linearizable=%v",
		name, nodes, clients, mix, batching, fsync, r.OpsPerSec, r.P50ms, r.P99ms, r.Linearizable)
	return r
}

// failover: kill the leader with kill -9 over and over while clients write; how long until a write
// is answered again?
func failover(trials int) map[string]any {
	c := newCluster(5)
	defer c.stop()
	start := time.Now()
	every := 3 * time.Second
	done := make(chan struct{})
	var hist []linear.Op
	go func() {
		hist, _ = client.Run(c.nodes, client.Workload{Clients: 8, Keys: 10, Duration: time.Duration(trials+1) * every,
			Seed: 99, Mix: "write", Start: start, MaxBackoff: 20 * time.Millisecond})
		close(done)
	}()
	var kills []int64
	for k := 0; k < trials; k++ {
		time.Sleep(every - 1200*time.Millisecond)
		l := c.waitLeader(5 * time.Second)
		kills = append(kills, time.Since(start).Nanoseconds())
		c.kill(l)
		time.Sleep(1200 * time.Millisecond)
		c.start(l)
	}
	<-done
	// writes work again at the first answer to a request sent after the kill (an answer to an
	// earlier request may already have been on its way when the leader died)
	var gaps []float64
	for _, k := range kills {
		first := int64(-1)
		for _, o := range hist {
			if !o.Unknown && o.Call > k && (first < 0 || o.Return < first) {
				first = o.Return
			}
		}
		if first > 0 {
			gaps = append(gaps, float64(first-k)/1e6)
		}
	}
	lin := check(hist)
	log.Printf("failover: %d kills, gaps (ms) %v, linearizable=%v", len(kills), round(gaps), lin)
	return map[string]any{"trials": len(kills), "gaps_ms": gaps, "linearizable": lin, "election_timeout_ms": "200-400",
		"heartbeat_ms": 30}
}

// catch-up: a follower is down while the others take writes; how long until it has everything?
func catchup(down time.Duration) map[string]any {
	c := newCluster(5, "-compact", "1000")
	defer c.stop()
	l := c.waitLeader(5 * time.Second)
	victim := l%5 + 1
	done := make(chan struct{})
	var hist []linear.Op
	go func() {
		hist, _ = client.Run(c.nodes, client.Workload{Clients: 16, Keys: 10, Duration: down + 6*time.Second, Seed: 7, Mix: "write"})
		close(done)
	}()
	time.Sleep(time.Second)
	before, _ := c.status(victim)
	c.kill(victim)
	time.Sleep(down)
	ls, _ := c.status(c.leader())
	target := ls.Commit
	c.start(victim)
	t0 := time.Now()
	var vs status
	for time.Since(t0) < 20*time.Second {
		if s, ok := c.status(victim); ok && s.Applied >= target {
			vs = s
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	took := time.Since(t0)
	<-done
	lin := check(hist)
	r := map[string]any{"down_s": down.Seconds(), "behind_entries": target - before.Applied, "catchup_ms": ms(took),
		"via_snapshot": vs.Snap > 0, "linearizable": lin}
	log.Printf("catch-up after %s down: %d entries behind, caught up in %s (process start included), snapshot=%v",
		down, target-before.Applied, took.Round(time.Millisecond), vs.Snap > 0)
	return r
}

func simulate(seeds int) []map[string]any {
	var res []map[string]any
	for _, name := range []string{"calm", "flaky-mobile", "power-cuts", "brownouts", "split-network", "everything"} {
		var ops, unknown, elections, crashes, splits, failed int
		var p50, p99 []int64
		for s := 1; s <= seeds; s++ {
			r := sim.New(sim.Config{Nodes: 5, Clients: 6, Keys: 4, Ticks: 4000, Seed: int64(s), Faults: sim.Profiles[name],
				ReadIndex: true, CompactEvery: 64}).Run()
			ops, unknown, elections, crashes, splits = ops+r.Completed, unknown+r.Unknown, elections+r.Elections, crashes+r.Crashes, splits+r.Partitions
			if !r.OK() {
				failed++
			}
			p50, p99 = append(p50, r.LatencyP50), append(p99, r.LatencyP99)
		}
		med := func(v []int64) float64 {
			sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
			return float64(v[len(v)/2]) * 10
		}
		simSecs := float64(seeds) * 40
		res = append(res, map[string]any{"profile": name, "runs": seeds, "answered": ops, "unanswered": unknown,
			"ops_per_sim_sec": float64(ops) / simSecs, "elections": elections, "crashes": crashes, "partitions": splits,
			"failed_runs": failed, "p50_ms": med(p50), "p99_ms": med(p99)})
		log.Printf("sim %-14s %d runs: %d answered, %d failed", name, seeds, ops, failed)
	}
	return res
}

func round(v []float64) []float64 {
	o := make([]float64, len(v))
	for i, x := range v {
		o[i] = float64(int(x))
	}
	return o
}

func fsyncMicros() float64 {
	f, _ := os.CreateTemp("", "fsync")
	defer os.Remove(f.Name())
	defer f.Close()
	t0 := time.Now()
	for i := 0; i < 200; i++ {
		f.Write(make([]byte, 256))
		f.Sync()
	}
	return float64(time.Since(t0).Microseconds()) / 200
}

func cpuModel() string {
	b, _ := os.ReadFile("/proc/cpuinfo")
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "model name") {
			return strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
	}
	return runtime.GOARCH
}

func main() {
	flag.Parse()
	clientsList, trials, seeds := []int{1, 4, 16, 64}, 12, 40
	if *quick {
		*runFor, clientsList, trials, seeds = 2*time.Second, []int{1, 16}, 3, 5
	}
	r := results{Machine: map[string]any{"cpus": runtime.NumCPU(), "cpu": cpuModel(), "go": runtime.Version(),
		"os": runtime.GOOS, "fsync_us": fsyncMicros(), "date": time.Now().UTC().Format(time.RFC3339),
		"note": "all five servers and all clients share this one machine; network is loopback"}}
	if host := os.Getenv("BENCH_HOST"); host != "" {
		r.Machine["host"] = host
	}
	for _, n := range []int{3, 5} {
		for _, c := range clientsList {
			r.Scaling = append(r.Scaling, load("scaling", n, c, "mixed", true, true))
		}
	}
	for _, mix := range []string{"read", "mixed", "write"} {
		r.Mix = append(r.Mix, load("mix", 5, 16, mix, true, true))
	}
	for _, v := range []struct{ batch, sync bool }{{true, true}, {false, true}, {true, false}, {false, false}} {
		r.Ablation = append(r.Ablation, load("ablation", 5, 16, "write", v.batch, v.sync))
	}
	r.Failover = failover(trials)
	for _, d := range []time.Duration{2 * time.Second, 6 * time.Second} {
		r.Catchup = append(r.Catchup, catchup(d))
	}
	r.Simulation = simulate(seeds)
	os.MkdirAll(filepath.Dir(*out), 0o755)
	b, _ := json.MarshalIndent(r, "", "  ")
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s", *out)
}
