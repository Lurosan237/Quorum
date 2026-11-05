// Package client talks to a Quorum cluster over HTTP, and can drive a workload against it while
// recording every request as a history for the linearizability checker.
package client

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Lurosan237/Quorum/linear"
)

// Client is one session: its writes are numbered, and a retried write keeps its number, so the
// cluster applies it once however many times it is sent.
type Client struct {
	ID         uint64
	nodes      map[int]string
	ids        []int
	leader     int
	seq        uint64
	http       *http.Client
	rng        *rand.Rand
	Timeout    time.Duration // give up on a request after this long (its outcome is then unknown)
	MaxBackoff time.Duration // longest wait between retries
}

// New makes a client for the cluster nodes (id -> base URL).
func New(id uint64, nodes map[int]string) *Client {
	ids := make([]int, 0, len(nodes))
	for n := range nodes {
		ids = append(ids, n)
	}
	sort.Ints(ids)
	return &Client{ID: id, nodes: nodes, ids: ids, leader: -1, Timeout: 5 * time.Second, MaxBackoff: 200 * time.Millisecond,
		http: &http.Client{Timeout: 2 * time.Second}, rng: rand.New(rand.NewSource(int64(id)))}
}

// ParseNodes reads "1=http://a:7001,2=http://b:7001".
func ParseNodes(s string) (map[int]string, error) {
	nodes := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		id, url, ok := strings.Cut(strings.TrimSpace(part), "=")
		n, err := strconv.Atoi(id)
		if !ok || err != nil {
			return nil, fmt.Errorf("bad node %q (want id=url)", part)
		}
		nodes[n] = strings.TrimRight(url, "/")
	}
	return nodes, nil
}

// Do runs one request ("put", "append" or "get") and returns the answer; ok is false when the
// client gave up without learning the outcome.
func (c *Client) Do(kind, key, value string) (string, bool) {
	var seq uint64
	if kind != "get" {
		c.seq++
		seq = c.seq
	}
	deadline := time.Now().Add(c.Timeout)
	backoff := min(10*time.Millisecond, c.MaxBackoff)
	for time.Now().Before(deadline) {
		target := c.leader
		if _, ok := c.nodes[target]; !ok {
			target = c.ids[c.rng.Intn(len(c.ids))]
		}
		method := map[string]string{"get": "GET", "put": "PUT", "append": "POST"}[kind]
		req, _ := http.NewRequest(method, c.nodes[target]+"/kv/"+key, strings.NewReader(value))
		if seq > 0 {
			req.Header.Set("X-Client", strconv.FormatUint(c.ID, 10))
			req.Header.Set("X-Seq", strconv.FormatUint(seq, 10))
		}
		resp, err := c.http.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusOK:
				c.leader = target
				return string(body), true
			case http.StatusMisdirectedRequest:
				if l, err := strconv.Atoi(resp.Header.Get("X-Leader")); err == nil && l != target {
					c.leader = l
					if _, known := c.nodes[l]; known {
						continue // straight to the leader it named
					}
				}
			}
		}
		c.leader = -1
		time.Sleep(backoff + time.Duration(c.rng.Int63n(int64(backoff))))
		if backoff *= 2; backoff > c.MaxBackoff {
			backoff = c.MaxBackoff
		}
	}
	return "", false
}

// Workload settings.
type Workload struct {
	Clients    int
	Keys       int
	Duration   time.Duration
	Seed       int64
	Start      time.Time     // the clock histories are timed on (zero = when Run starts)
	MaxBackoff time.Duration // longest wait between retries (zero = 200 ms)
	Mix        string        // "mixed" (default: 40% gets, 25% puts, 35% appends), "write" (puts) or "read" (gets)
}

// Stats of a run.
type Stats struct {
	Ops, OK, Unknown int
	Latencies        []time.Duration // of the answered requests
}

// Run drives the cluster with concurrent clients (40% gets, 25% puts, 35% appends, every value
// unique) and returns everything that happened, timed on one clock.
func Run(nodes map[int]string, w Workload) ([]linear.Op, Stats) {
	start := w.Start
	if start.IsZero() {
		start = time.Now()
	}
	var (
		mu    sync.Mutex
		hist  []linear.Op
		stats Stats
		wg    sync.WaitGroup
	)
	for i := 0; i < w.Clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := New(uint64(w.Seed)*1000+uint64(i)+1, nodes)
			if w.MaxBackoff > 0 {
				c.MaxBackoff = w.MaxBackoff
			}
			rng := rand.New(rand.NewSource(w.Seed*7919 + int64(i)))
			for n := 0; time.Since(start) < w.Duration; n++ {
				key := fmt.Sprintf("k%d", rng.Intn(w.Keys))
				kind, value := "get", ""
				r := rng.Intn(100)
				switch w.Mix {
				case "write":
					r = 0
				case "read":
					r = 99
				}
				switch {
				case r < 25:
					kind, value = "put", fmt.Sprintf("c%d.%d;", i, n)
				case r < 60:
					kind, value = "append", fmt.Sprintf("c%d.%d;", i, n)
				}
				call := time.Since(start)
				out, ok := c.Do(kind, key, value)
				ret := time.Since(start)
				op := linear.Op{Client: c.ID, Kind: kind, Key: key, Value: value, Output: out,
					Call: call.Nanoseconds(), Return: ret.Nanoseconds(), Unknown: !ok}
				mu.Lock()
				hist = append(hist, op)
				stats.Ops++
				if ok {
					stats.OK++
					stats.Latencies = append(stats.Latencies, ret-call)
				} else {
					stats.Unknown++
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return hist, stats
}

// Percentile of the answered requests' latencies (p in 0..100).
func (s Stats) Percentile(p float64) time.Duration {
	if len(s.Latencies) == 0 {
		return 0
	}
	l := append([]time.Duration(nil), s.Latencies...)
	sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
	return l[int(float64(len(l)-1)*p/100)]
}
