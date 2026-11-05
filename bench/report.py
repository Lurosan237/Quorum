"""Turn bench/results.json (written by cmd/quorum-bench) into bench/REPORT.md and its charts.

    python bench/report.py

Every number in the report comes from the JSON file; nothing is typed by hand.
"""

import json
import statistics
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = Path(__file__).resolve().parent
R = json.loads((HERE / "results.json").read_text())

# reference palette (light): surface, inks, categorical slots in fixed order
SURFACE, INK, INK2, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#8a8984", "#e6e5e0"
S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
    "font.size": 10.5, "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlecolor": INK,
    "axes.titlelocation": "left", "legend.frameon": False, "legend.labelcolor": INK2,
})


def save(fig, name):
    fig.tight_layout()
    fig.savefig(HERE / name, dpi=160)
    plt.close(fig)


# ---- charts ----------------------------------------------------------------------------------------

def chart_scaling():
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 3.8))
    for nodes, color in ((3, S1), (5, S2)):
        rows = [r for r in R["scaling"] if r["nodes"] == nodes]
        xs = [r["clients"] for r in rows]
        a.plot(xs, [r["ops_per_sec"] for r in rows], color=color, lw=2, marker="o", ms=6, label=f"{nodes} servers")
        b.plot(xs, [r["p99_ms"] for r in rows], color=color, lw=2, marker="o", ms=6, label=f"{nodes} servers")
        b.plot(xs, [r["p50_ms"] for r in rows], color=color, lw=1.5, ls="--", marker="o", ms=4)
        a.annotate(f"{nodes} servers", (xs[-1], rows[-1]["ops_per_sec"]), xytext=(6, 0), textcoords="offset points",
                   va="center", color=INK2)
    for ax in (a, b):
        ax.set_xscale("log", base=2)
        ax.set_xticks([r["clients"] for r in R["scaling"] if r["nodes"] == 3])
        ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax.set_xlabel("concurrent clients")
    a.set_title("Answers per second (mixed reads and writes)")
    a.set_ylim(bottom=0)
    b.set_title("Response time, ms")
    b.plot([], [], color=INK2, lw=2, label="99th percentile")
    b.plot([], [], color=INK2, lw=1.5, ls="--", label="median")
    b.set_ylim(bottom=0)
    b.legend(loc="upper left")
    save(fig, "scaling.png")


def chart_ablation():
    rows = R["ablation"]
    labels = [("batching" if r["batching"] else "no batching") + "\n" + ("fsync" if r["fsync"] else "no fsync (unsafe)")
              for r in rows]
    fig, ax = plt.subplots(figsize=(7, 3.4))
    colors = [S1 if r["fsync"] else MUTED for r in rows]
    bars = ax.bar(labels, [r["ops_per_sec"] for r in rows], color=colors, width=0.6)
    for b, r in zip(bars, rows):
        ax.annotate(f'{r["ops_per_sec"]:,.0f}', (b.get_x() + b.get_width() / 2, b.get_height()), xytext=(0, 4),
                    textcoords="offset points", ha="center", color=INK)
    ax.set_title("Writes per second, 5 servers, 16 clients")
    ax.grid(axis="x", visible=False)
    save(fig, "ablation.png")


def chart_failover():
    gaps = R["failover"]["gaps_ms"]
    fig, ax = plt.subplots(figsize=(7, 2.4))
    ax.scatter(gaps, [((i * 7) % 5 - 2) * 0.12 for i in range(len(gaps))], s=60, color=S1, edgecolor=SURFACE, linewidth=2, zorder=3)
    med = statistics.median(gaps)
    ax.axvline(med, color=INK2, lw=1, ls="--")
    ax.annotate(f"median {med:.0f} ms", (med, 0.6), xytext=(6, 0), textcoords="offset points", color=INK2)
    ax.set_yticks([])
    ax.set_ylim(-1, 1)
    ax.set_xlim(left=0)
    ax.set_xlabel("ms from kill -9 of the leader to the first write answered again")
    ax.set_title(f"Failover, {len(gaps)} trials")
    ax.grid(axis="y", visible=False)
    save(fig, "failover.png")


# ---- the report ------------------------------------------------------------------------------------

def table(rows, cols):
    head = "| " + " | ".join(c[0] for c in cols) + " |\n|" + "|".join("---:" if c[2] else "---" for c in cols) + "|\n"
    return head + "".join("| " + " | ".join(c[1](r) for c in cols) + " |\n" for r in rows)


def yes(b):
    return "yes" if b else "**NO**"


def main():
    chart_scaling()
    chart_ablation()
    chart_failover()
    m = R["machine"]
    every = R["scaling"] + R["mix"] + R["ablation"]
    all_lin = all(r["linearizable"] for r in every) and R["failover"]["linearizable"] and all(
        c["linearizable"] for c in R["catchup"])
    total_ops = sum(r["answered"] for r in every)
    gaps = R["failover"]["gaps_ms"]
    abl = {(r["batching"], r["fsync"]): r for r in R["ablation"]}
    best = max(R["scaling"], key=lambda r: r["ops_per_sec"])
    sim = R["simulation"]
    top = max(r["clients"] for r in R["scaling"])
    t3 = next(r for r in R["scaling"] if r["nodes"] == 3 and r["clients"] == top)["ops_per_sec"]
    t5 = next(r for r in R["scaling"] if r["nodes"] == 5 and r["clients"] == top)["ops_per_sec"]
    five_vs_three = (f"At {top} clients five servers answer {t5 / t3:.0%} as many requests as three: each write is "
                     f"copied to four followers instead of two, and here they all share the same {m['cpus']} CPUs.")

    load_cols = [
        ("servers", lambda r: str(r["nodes"]), True), ("clients", lambda r: str(r["clients"]), True),
        ("mix", lambda r: r["mix"], False), ("answers/s", lambda r: f'{r["ops_per_sec"]:,.0f}', True),
        ("median ms", lambda r: f'{r["p50_ms"]:.2f}', True), ("p90 ms", lambda r: f'{r["p90_ms"]:.2f}', True),
        ("p99 ms", lambda r: f'{r["p99_ms"]:.2f}', True), ("unknown", lambda r: str(r["unknown"]), True),
        ("linearizable", lambda r: yes(r["linearizable"]), False)]

    md = f"""# Quorum benchmarks

Generated by `bench/report.py` from [`results.json`](results.json), which `cmd/quorum-bench` wrote on
{m["date"][:10]}. Rerun both to reproduce (see the end of this page).

**Machine.** {m["cpus"]} CPUs ({m["cpu"]}), {m["go"]}, one fsync of a small write takes {m["fsync_us"]:.0f} µs.
{m.get("host", "")} All servers and all clients run on this one machine and talk over loopback HTTP, so
these numbers measure the software, not a network: a real deployment adds the round trip between
machines to every write.

**Correctness first.** Every load run below was recorded and checked by the linearizability checker:
{total_ops:,} answers in the load runs, {"all linearizable" if all_lin else "**NOT ALL LINEARIZABLE**"}. A
fast wrong answer would not count.

## Throughput and response time

![Throughput and response time against concurrent clients](scaling.png)

{table(R["scaling"], load_cols)}
The best run here was {best["ops_per_sec"]:,.0f} answers a second ({best["nodes"]} servers, {best["clients"]}
clients). {five_vs_three} One client sends one request at a time, so its rate is set by response time;
more clients keep the leader's batches full.

## Reads against writes

{table(R["mix"], load_cols)}
Reads skip the log: the leader confirms it is still the leader with one heartbeat round (ReadIndex)
and answers from memory, with no disk write.

## What batching and fsync cost

![Writes per second with and without batching and fsync](ablation.png)

{table(R["ablation"], [("batching", lambda r: "on" if r["batching"] else "off", False),
                       ("fsync", lambda r: "on" if r["fsync"] else "off (unsafe)", False)] + load_cols[3:])}
Every write must be on disk before it is acknowledged, or a power cut could lose it. Done one request
at a time that is one fsync per write: {abl[(False, True)]["ops_per_sec"]:,.0f} writes a second. The leader
instead takes every request already waiting and saves them with one fsync:
{abl[(True, True)]["ops_per_sec"]:,.0f} a second, {abl[(True, True)]["ops_per_sec"] / abl[(False, True)]["ops_per_sec"]:.1f}× more, with
the same guarantee. Turning fsync off is shown only to measure its cost; the server never runs that way
unless told to with `-no-fsync`.

## Failover

![Time from killing the leader to the first answered write](failover.png)

The leader was killed with `kill -9` {R["failover"]["trials"]} times while 8 clients wrote, and restarted a
second later. Time until a write sent after the kill was answered: median {statistics.median(gaps):.0f} ms,
fastest {min(gaps):.0f} ms, slowest {max(gaps):.0f} ms. Each follower waits a random {R["failover"]["election_timeout_ms"]} ms
without a heartbeat before it starts an election, and the first of the four to time out usually wins,
so most of this is that wait; the rest is a pre-vote round, a vote round and the new leader's first
commit. A shorter timeout would fail over faster but would also start needless elections on a slow
network, which is the trade-off the flaky-mobile scenario exists to test. History linearizable:
{yes(R["failover"]["linearizable"])}.

## Catching up after an outage

{table(R["catchup"], [("down for", lambda c: f'{c["down_s"]:.0f} s', True),
                      ("entries missed", lambda c: f'{c["behind_entries"]:,}', True),
                      ("back in step after", lambda c: f'{c["catchup_ms"]:.0f} ms', True),
                      ("by snapshot", lambda c: "yes" if c["via_snapshot"] else "no", False),
                      ("linearizable", lambda c: yes(c["linearizable"]), False)])}
A follower was killed while 16 clients kept writing, then started again. "Back in step" is when it had
applied everything the leader had committed at the moment it restarted; the time includes starting
the process and replaying its own log from disk. When it has missed more than the leader still keeps in
its log, the leader sends a snapshot of the whole store instead of the entries.

## The simulator

{sim[0]["runs"]} seeded runs per scenario, each 40 simulated seconds with 5 servers and 6 clients, reads
served with ReadIndex. Response times are in simulated milliseconds (one tick is 10 ms; message delays
are 1 to 12 ticks depending on the scenario).

{table(sim, [("scenario", lambda s: s["profile"], False), ("answers", lambda s: f'{s["answered"]:,}', True),
             ("per sim. second", lambda s: f'{s["ops_per_sim_sec"]:.0f}', True),
             ("median ms", lambda s: f'{s["p50_ms"]:.0f}', True), ("p99 ms", lambda s: f'{s["p99_ms"]:.0f}', True),
             ("leaders elected", lambda s: str(s["elections"]), True), ("power cuts", lambda s: str(s["crashes"]), True),
             ("splits", lambda s: str(s["partitions"]), True),
             ("failed runs", lambda s: str(s["failed_runs"]), True)])}
A failed run would be any safety violation or any history that is not linearizable. Fewer answers
under flaky mobile data and "everything" is expected: lost messages mean retries and elections.

## Reproduce

```
go build -o bin/quorum-server ./cmd/quorum-server
go run ./cmd/quorum-bench -server bin/quorum-server -out bench/results.json
python bench/report.py
```

The `Bench` workflow in `.github/workflows/bench.yml` runs exactly this on a GitHub runner.
"""
    (HERE / "REPORT.md").write_text(md)
    print("wrote", HERE / "REPORT.md", "(all linearizable)" if all_lin else "(NOT ALL LINEARIZABLE)")
    return 0 if all_lin else 1


if __name__ == "__main__":
    sys.exit(main())
