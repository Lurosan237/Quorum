// Package kv is the key-value state machine replicated by Raft: put, append and get on string
// keys, with client sessions so that a request retried after a timeout (which may or may not have
// been applied the first time) is applied exactly once.
package kv

import (
	"encoding/json"
	"fmt"
)

// Kind of operation.
type Kind string

const (
	Put    Kind = "put"
	Append Kind = "append"
	Get    Kind = "get"
)

// Op is one client request. Client and Seq identify it: a client sends its requests one at a time,
// numbered 1, 2, 3...; a retry carries the same Seq.
type Op struct {
	Kind   Kind   `json:"k"`
	Key    string `json:"key"`
	Value  string `json:"v,omitempty"`
	Client uint64 `json:"c"`
	Seq    uint64 `json:"s"`
}

func (o Op) String() string {
	switch o.Kind {
	case Get:
		return fmt.Sprintf("get(%s)", o.Key)
	default:
		return fmt.Sprintf("%s(%s, %q)", o.Kind, o.Key, o.Value)
	}
}

// Encode turns an op into a log entry's data.
func (o Op) Encode() []byte {
	b, _ := json.Marshal(o)
	return b
}

// Decode reads an op from a log entry.
func Decode(b []byte) (Op, error) {
	var o Op
	err := json.Unmarshal(b, &o)
	return o, err
}

type session struct {
	Seq    uint64 `json:"s"`
	Result string `json:"r"`
}

// Store is the state machine.
type Store struct {
	Data     map[string]string   `json:"data"`
	Sessions map[uint64]session  `json:"sessions"`
	Applied  uint64              `json:"applied"`
}

func New() *Store {
	return &Store{Data: map[string]string{}, Sessions: map[uint64]session{}}
}

// Apply runs one committed op and returns its result (the value, for a get; the value after the
// write, for put and append). A duplicate of the client's last op returns the saved result without
// applying it again; an older one is a stale retry the client no longer waits for.
func (s *Store) Apply(index uint64, o Op) (result string, duplicate bool) {
	s.Applied = index
	if sess, ok := s.Sessions[o.Client]; ok && o.Seq <= sess.Seq {
		return sess.Result, true
	}
	switch o.Kind {
	case Put:
		s.Data[o.Key] = o.Value
	case Append:
		s.Data[o.Key] += o.Value
	}
	result = s.Data[o.Key]
	if o.Client != 0 {
		s.Sessions[o.Client] = session{Seq: o.Seq, Result: result}
	}
	return result, false
}

// Read serves a get without the log (after a ReadIndex): no session update.
func (s *Store) Read(key string) string { return s.Data[key] }

// Snapshot serialises the whole state machine.
func (s *Store) Snapshot() []byte {
	b, _ := json.Marshal(s)
	return b
}

// Restore replaces the state machine with a snapshot.
func Restore(b []byte) (*Store, error) {
	s := New()
	if len(b) == 0 {
		return s, nil
	}
	err := json.Unmarshal(b, s)
	if s.Data == nil {
		s.Data = map[string]string{}
	}
	if s.Sessions == nil {
		s.Sessions = map[uint64]session{}
	}
	return s, err
}
