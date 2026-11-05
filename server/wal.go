package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Lurosan237/Quorum/raft"
)

// wal is the write-ahead log: everything Raft must find again after a crash, one JSON record per
// line, fsynced before any message that depends on it is sent. When a snapshot is taken the file
// is rewritten (snapshot + the entries after it) and swapped in atomically by a rename.
type wal struct {
	path   string
	f      *os.File
	w      *bufio.Writer
	recs   int
	noSync bool
}

type walRecord struct {
	HardState *raft.HardState `json:"hs,omitempty"`
	From      uint64          `json:"from,omitempty"` // entries replace everything from this index on
	Entries   []raft.Entry    `json:"ents,omitempty"`
	Snapshot  *raft.Snapshot  `json:"snap,omitempty"`
}

// openWAL reads what a previous run saved (if anything) and opens the file for appending.
func openWAL(dir string) (*wal, raft.Persisted, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, raft.Persisted{}, err
	}
	path := filepath.Join(dir, "wal.jsonl")
	p := raft.Persisted{HardState: raft.HardState{Vote: -1}}
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<30)
		for sc.Scan() {
			var r walRecord
			if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
				break // a torn last line from a crash mid-write: everything before it stands
			}
			apply(&p, r)
		}
		f.Close()
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, p, err
	}
	return &wal{path: path, f: f, w: bufio.NewWriter(f)}, p, nil
}

func apply(p *raft.Persisted, r walRecord) {
	if r.Snapshot != nil {
		p.Snapshot = r.Snapshot
		var keep []raft.Entry
		for _, e := range p.Entries {
			if e.Index > r.Snapshot.Index {
				keep = append(keep, e)
			}
		}
		p.Entries = keep
	}
	if len(r.Entries) > 0 {
		var keep []raft.Entry
		for _, e := range p.Entries {
			if e.Index < r.From {
				keep = append(keep, e)
			}
		}
		p.Entries = append(keep, r.Entries...)
	}
	if r.HardState != nil {
		p.HardState = *r.HardState
	}
}

// save writes one Ready's worth of state and makes it durable.
func (w *wal) save(rd raft.Ready) error {
	rec := walRecord{}
	if rd.MustSave {
		hs := rd.HardState
		rec.HardState = &hs
	}
	if len(rd.Entries) > 0 {
		rec.From, rec.Entries = rd.EntriesFrom, rd.Entries
	}
	if rd.Snapshot != nil {
		rec.Snapshot = rd.Snapshot
	}
	if rec.HardState == nil && rec.Entries == nil && rec.Snapshot == nil {
		return nil
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	w.w.Write(b)
	w.w.WriteByte('\n')
	if err := w.w.Flush(); err != nil {
		return err
	}
	w.recs++
	if w.noSync {
		return nil
	}
	return w.f.Sync()
}

// rewrite replaces the file with the node's current saved state (after a snapshot), so the log
// does not grow forever.
func (w *wal) rewrite(p raft.Persisted) error {
	tmp := w.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	for _, r := range []walRecord{{Snapshot: p.Snapshot}, {HardState: &p.HardState}, {From: firstIndex(p), Entries: p.Entries}} {
		if r.Snapshot == nil && r.HardState == nil && len(r.Entries) == 0 {
			continue
		}
		b, _ := json.Marshal(r)
		bw.Write(b)
		bw.WriteByte('\n')
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	f.Close()
	w.f.Close()
	if err := os.Rename(tmp, w.path); err != nil {
		return fmt.Errorf("swap wal: %w", err)
	}
	nf, err := os.OpenFile(w.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w.f, w.w, w.recs = nf, bufio.NewWriter(nf), 0
	return nil
}

func firstIndex(p raft.Persisted) uint64 {
	if len(p.Entries) > 0 {
		return p.Entries[0].Index
	}
	return 0
}
