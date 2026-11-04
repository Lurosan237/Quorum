# Quorum

A replicated key-value store on Raft, written from scratch in Go, built to stay correct on
unreliable networks: power cuts, brownouts, flaky mobile data and a network split in two.

> **Work in progress.** Layer 1 (Raft, the store, the deterministic simulator and a
> linearizability checker) is in.

```
go test ./...                                         # every scenario, many seeds, both read modes
go run ./cmd/quorum-sim -profile all -seeds 200       # the big sweep
go run ./cmd/quorum-sim -profile brownouts -seed 238 -bug forget-vote -v   # watch a bug get caught
```
