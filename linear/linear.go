// Package linear checks that a history of key-value operations is linearizable: that there is one
// order of all operations, consistent with real time (an operation that finished before another
// started comes first), in which every result is what a single, perfectly reliable copy of the
// store would have returned.
//
// The search is the Wing & Gong / Lowe algorithm: take, in turn, each operation that could come
// next (one invoked before the earliest return among those not yet placed), apply it to a model of
// the store, and backtrack on a wrong result; states already explored are remembered. A key-value
// store is checked one key at a time (linearizability is compositional), which keeps every search
// small.
//
// Operations whose outcome the client never learned (it timed out and gave up) may have taken
// effect at any point after they were invoked, or never: the search may place them or skip them.
package linear

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// Op is one completed (or abandoned) client request.
type Op struct {
	Client  uint64
	Kind    string // "put", "append" or "get"
	Key     string
	Value   string // put / append: the value written
	Output  string // what the client was told (the value read, or the value after a write)
	Call    int64  // when it was sent
	Return  int64  // when the answer arrived
	Unknown bool   // no answer: the client does not know whether it happened
}

func (o Op) String() string {
	out := o.Output
	if o.Unknown {
		out = "?"
	}
	switch o.Kind {
	case "get":
		return fmt.Sprintf("client %d: get(%s) -> %q  [%d, %d]", o.Client, o.Key, out, o.Call, o.Return)
	default:
		return fmt.Sprintf("client %d: %s(%s, %q) -> %q  [%d, %d]", o.Client, o.Kind, o.Key, o.Value, out, o.Call, o.Return)
	}
}

// Result of a check.
type Result struct {
	OK        bool
	Keys      int
	Ops       int
	Explored  int    // states visited by the search
	BadKey    string // first key whose history is not linearizable
	Explain   string // the operations on that key, in call order
	GaveUp    bool   // search budget exhausted (inconclusive, never reported as a pass)
}

// Check checks a history. budget caps the states explored per key (0 = a large default).
func Check(history []Op, budget int) Result {
	if budget == 0 {
		budget = 5_000_000
	}
	byKey := map[string][]Op{}
	for _, o := range history {
		byKey[o.Key] = append(byKey[o.Key], o)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	res := Result{OK: true, Keys: len(keys), Ops: len(history)}
	for _, k := range keys {
		ok, explored, gaveUp := checkKey(byKey[k], budget)
		res.Explored += explored
		if gaveUp {
			res.GaveUp, res.OK = true, false
			res.BadKey = k
			return res
		}
		if !ok {
			res.OK, res.BadKey = false, k
			res.Explain = explain(byKey[k])
			return res
		}
	}
	return res
}

func explain(ops []Op) string {
	sorted := append([]Op(nil), ops...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Call < sorted[j].Call })
	var b strings.Builder
	for _, o := range sorted {
		b.WriteString(o.String())
		b.WriteByte('\n')
	}
	return b.String()
}

// apply runs op on the model value and says whether its output matches what the client saw.
func apply(state string, o Op) (string, bool) {
	switch o.Kind {
	case "put":
		return o.Value, o.Unknown || o.Output == o.Value
	case "append":
		next := state + o.Value
		return next, o.Unknown || o.Output == next
	default:
		return state, o.Unknown || o.Output == state
	}
}

func checkKey(ops []Op, budget int) (ok bool, explored int, gaveUp bool) {
	n := len(ops)
	ret := make([]int64, n)
	for i, o := range ops {
		ret[i] = o.Return
		if o.Unknown {
			ret[i] = math.MaxInt64 // may have happened at any time from its call on
		}
	}
	words := (n + 63) / 64
	seen := map[string]bool{}
	done := make([]uint64, words)
	known := 0
	for _, o := range ops {
		if !o.Unknown {
			known++
		}
	}

	var dfs func(state string, placedKnown int) bool
	dfs = func(state string, placedKnown int) bool {
		if placedKnown == known {
			return true // every answered op is placed; unanswered ones may simply not have happened
		}
		explored++
		if explored > budget {
			gaveUp = true
			return false
		}
		key := keyOf(done, state)
		if seen[key] {
			return false
		}
		seen[key] = true
		// the earliest return among unplaced ops bounds which ops can go next
		minRet := int64(math.MaxInt64)
		for i := 0; i < n; i++ {
			if done[i/64]&(1<<(i%64)) == 0 && ret[i] < minRet {
				minRet = ret[i]
			}
		}
		for i := 0; i < n; i++ {
			if done[i/64]&(1<<(i%64)) != 0 || ops[i].Call > minRet {
				continue
			}
			next, match := apply(state, ops[i])
			if !match {
				continue
			}
			done[i/64] |= 1 << (i % 64)
			add := 0
			if !ops[i].Unknown {
				add = 1
			}
			if dfs(next, placedKnown+add) {
				return true
			}
			done[i/64] &^= 1 << (i % 64)
			if gaveUp {
				return false
			}
		}
		return false
	}
	ok = dfs("", 0)
	return ok, explored, gaveUp
}

func keyOf(done []uint64, state string) string {
	var b strings.Builder
	for _, w := range done {
		fmt.Fprintf(&b, "%x,", w)
	}
	b.WriteString(state)
	return b.String()
}
