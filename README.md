# Quorum

A replicated key-value store on Raft, written from scratch in Go with only the standard library, and
built around one condition: **the power keeps going out**.

I study in Bertoua, Cameroon, where power cuts and internet outages are part of the week. Quorum is
the store I wanted for that: five servers keep one copy of the truth between them, and every answer any
client gets must fit one order of events, even while servers lose power, get cut off and come back.

**[Try the live demo](https://lurosan237.github.io/Quorum/)**: five servers running in your browser.
Cut a server's power or network, change the weather, send your own requests, then check every answer.

[![CI](https://github.com/Lurosan237/Quorum/actions/workflows/ci.yml/badge.svg)](https://github.com/Lurosan237/Quorum/actions/workflows/ci.yml)

| | |
|---|---|
| Correctness | 455,266 benchmark answers and every chaos run checked by a linearizability checker: all linearizable |
| Failover | median 236 ms after `kill -9` of the leader ([report](bench/REPORT.md#failover)) |
| Throughput | up to 8,483 answers/s on one 4-CPU machine, every write fsynced before it is acknowledged |
| Chaos in CI | five Docker containers killed, frozen, partitioned, slowed and blacked out for two minutes on every push |

## How it is built

```
            simulator            real server              browser demo
        (seeded faults,       (HTTP, fsynced WAL,      (WebAssembly, you
         invariants)           Docker chaos test)       break it yourself)
                 \                    |                      /
                  +------ raft/ + kv/: the same code -------+
                          Tick, Step in; Ready out
                          no clock, no network, no disk inside
```

The Raft core is a pure state machine: it is told that time passed (`Tick`) or a message arrived
(`Step`), and hands back a `Ready` saying what to save, what to send and what is committed. Because it
does no I/O, the exact same code runs in the deterministic simulator, the real server and the browser.

| Package | What it is |
|---|---|
| [`raft/`](raft) | Leader election with PreVote and CheckQuorum, log replication with fast backtracking, snapshots and compaction, ReadIndex reads. A new leader commits a no-op from its own term. |
| [`kv/`](kv) | The store: put, append, get. Client sessions (client id + sequence number) make retried writes apply once. |
| [`sim/`](sim) | A deterministic world: one seed decides every delay, drop, duplicate, partition and power cut. Checks election safety and state-machine safety after every step. |
| [`linear/`](linear) | Linearizability checker (Wing & Gong search with memoization, per key). Requests whose answer was lost may or may not have happened. |
| [`server/`](server) | A real node: one goroutine owns Raft and the store; HTTP between servers; a JSON-lines write-ahead log, fsynced before any message that depends on it, tolerant of a torn last record. Requests already waiting share one fsync. |
| [`client/`](client) | HTTP client with leader discovery and retries; drives concurrent workloads and records histories. |
| [`cmd/`](cmd) | `quorum-server`, `quorum-check` (load + linearizability check against a live cluster), `quorum-sim`, `quorum-bench`, `quorum-wasm`. |
| [`site/`](site) | The demo page. |
| [`tools/netwatch`](tools/netwatch) | Records a week of my real internet and power, to replay in the simulator as a Bertoua scenario. |

## How it is tested

1. **Simulator sweep.** Six scenarios (calm, flaky mobile data, power cuts, brownouts, split network,
   everything at once), many seeds each, both read modes; invariants checked every step, every history
   checked for linearizability. Any run replays exactly from its seed.
2. **Planted bugs.** Two deliberate bugs run in CI (no client sessions; a vote forgotten across a power
   cut) and must be caught, so the checks are known to fail when they should.
3. **Real servers.** Unit tests for the log (torn writes, rewrite after snapshot) and an in-process
   cluster restarted under load.
4. **Chaos.** [`scripts/chaos.sh`](scripts/chaos.sh) runs five servers in Docker and, at random, kills
   them with `kill -9`, freezes them, cuts them off the network, adds latency and loss with `tc netem`,
   kills three of five, and blacks out all five at once, while ten clients write and read. The whole
   history must be linearizable. CI runs it for two minutes on every push.

## Run it

```sh
go test -race ./...                                   # everything above except Docker
go run ./cmd/quorum-sim -profile all -seeds 50        # the simulator sweep
scripts/chaos.sh 60                                   # needs Docker

# a local cluster by hand
go build -o bin/quorum-server ./cmd/quorum-server
P=1=http://127.0.0.1:7001,2=http://127.0.0.1:7002,3=http://127.0.0.1:7003
for i in 1 2 3; do bin/quorum-server -id $i -peers $P -listen 127.0.0.1:700$i -data data/$i & done
curl -X PUT  -H 'X-Client: 1' -H 'X-Seq: 1' -d hello 127.0.0.1:7001/kv/greeting
curl 127.0.0.1:7001/kv/greeting                       # a follower answers 421 with X-Leader
go run ./cmd/quorum-check -nodes $P -duration 20s     # kill servers while it runs
```

## Benchmarks

[`bench/REPORT.md`](bench/REPORT.md) is generated by a script from the results of
[`cmd/quorum-bench`](cmd/quorum-bench), run on a GitHub runner: throughput and response time by
client count, reads against writes, what batching and fsync cost, failover, and catch-up after an
outage. All servers share one machine, so the numbers measure the software, not a network.

## Limits

Fixed membership (no joint consensus yet), snapshots sent as one message, client sessions that never
expire, and JSON on the wire and on disk. Next: the Bertoua scenario from my own recorded week, and
Club STEAM's records stored on Quorum.

## Author

Len ([Lurosan237](https://github.com/Lurosan237)), mathematics student at Université de Bertoua.
