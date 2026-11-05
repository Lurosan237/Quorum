//go:build js && wasm

// quorum-wasm is the simulator compiled for the browser: the demo page drives a live cluster with
// it, and can run whole seeded runs (with or without a deliberate bug) at full speed.
package main

import (
	"encoding/json"
	"syscall/js"

	"github.com/Lurosan237/Quorum/sim"
)

var world *sim.Sim

func jsonOf(v any) any {
	b, _ := json.Marshal(v)
	return string(b)
}

func profile(name string) sim.Faults {
	if f, ok := sim.Profiles[name]; ok {
		return f
	}
	return sim.Profiles["calm"]
}

func main() {
	api := map[string]any{
		// start(profile, seed, clients)
		"start": js.FuncOf(func(_ js.Value, a []js.Value) any {
			world = sim.New(sim.Config{Nodes: 5, Clients: a[2].Int(), Keys: 3, Seed: int64(a[1].Int()),
				Faults: profile(a[0].String()), ReadIndex: true, CompactEvery: 24, ClientTimeout: 60})
			return nil
		}),
		"step": js.FuncOf(func(_ js.Value, a []js.Value) any {
			for i := 0; i < a[0].Int(); i++ {
				world.Step()
			}
			return nil
		}),
		"view":    js.FuncOf(func(_ js.Value, a []js.Value) any { return jsonOf(world.View(a[0].Int())) }),
		"crash":   js.FuncOf(func(_ js.Value, a []js.Value) any { world.Crash(a[0].Int()); return nil }),
		"restart": js.FuncOf(func(_ js.Value, a []js.Value) any { world.Restart(a[0].Int()); return nil }),
		"isolate": js.FuncOf(func(_ js.Value, a []js.Value) any { world.Isolate(a[0].Int()); return nil }),
		"heal":    js.FuncOf(func(_ js.Value, a []js.Value) any { world.Heal(); return nil }),
		"weather": js.FuncOf(func(_ js.Value, a []js.Value) any { world.SetFaults(profile(a[0].String())); return nil }),
		"ask": js.FuncOf(func(_ js.Value, a []js.Value) any {
			return world.Ask(a[0].String(), a[1].String(), a[2].String())
		}),
		"check": js.FuncOf(func(_ js.Value, a []js.Value) any {
			r := world.Check()
			return jsonOf(map[string]any{"ok": r.OK && !r.GaveUp, "gaveUp": r.GaveUp, "ops": r.Ops, "keys": r.Keys,
				"explored": r.Explored, "badKey": r.BadKey, "explain": r.Explain, "violations": world.View(0).Violations})
		}),
		// run(profile, seed, ticks, bug): one whole run at full speed, as the tests do it
		"run": js.FuncOf(func(_ js.Value, a []js.Value) any {
			r := sim.New(sim.Config{Nodes: 5, Clients: 6, Keys: 3, Ticks: a[2].Int(), Seed: int64(a[1].Int()),
				Faults: profile(a[0].String()), ReadIndex: true, CompactEvery: 64, Bug: a[3].String()}).Run()
			return jsonOf(map[string]any{"ok": r.OK(), "completed": r.Completed, "elections": r.Elections,
				"crashes": r.Crashes, "partitions": r.Partitions, "violations": r.Violations, "linear": r.Linear.OK,
				"badKey": r.Linear.BadKey, "explain": r.Linear.Explain, "committed": r.Committed})
		}),
	}
	js.Global().Set("quorum", js.ValueOf(api))
	js.Global().Call("dispatchEvent", js.Global().Get("Event").New("quorum-ready"))
	select {}
}
