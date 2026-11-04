package linear

import "testing"

func TestOverlappingWritesCanBeOrderedEitherWay(t *testing.T) {
	h := []Op{
		{Client: 1, Kind: "put", Key: "x", Value: "a", Output: "a", Call: 0, Return: 10},
		{Client: 2, Kind: "put", Key: "x", Value: "b", Output: "b", Call: 5, Return: 15},
		{Client: 3, Kind: "get", Key: "x", Output: "a", Call: 20, Return: 25}, // b then a: fine, they overlapped
	}
	if r := Check(h, 0); !r.OK {
		t.Fatalf("expected linearizable: %+v", r)
	}
}

func TestAStaleReadIsCaught(t *testing.T) {
	h := []Op{
		{Client: 1, Kind: "put", Key: "x", Value: "a", Output: "a", Call: 0, Return: 10},
		{Client: 1, Kind: "put", Key: "x", Value: "b", Output: "b", Call: 11, Return: 20},
		{Client: 2, Kind: "get", Key: "x", Output: "a", Call: 30, Return: 35}, // b finished before: stale
	}
	r := Check(h, 0)
	if r.OK || r.BadKey != "x" {
		t.Fatalf("expected a violation on x: %+v", r)
	}
}

func TestAnAppendAppliedTwiceIsCaught(t *testing.T) {
	h := []Op{
		{Client: 1, Kind: "append", Key: "x", Value: "1", Output: "1", Call: 0, Return: 10},
		{Client: 2, Kind: "get", Key: "x", Output: "11", Call: 20, Return: 25},
	}
	if r := Check(h, 0); r.OK {
		t.Fatal("a duplicated append must not pass")
	}
}

func TestAnUnansweredWriteMayOrMayNotHaveHappened(t *testing.T) {
	lost := Op{Client: 1, Kind: "put", Key: "x", Value: "a", Call: 0, Unknown: true}
	seen := []Op{lost, {Client: 2, Kind: "get", Key: "x", Output: "a", Call: 50, Return: 60}}
	notSeen := []Op{lost, {Client: 2, Kind: "get", Key: "x", Output: "", Call: 50, Return: 60}}
	for _, h := range [][]Op{seen, notSeen} {
		if r := Check(h, 0); !r.OK {
			t.Fatalf("both outcomes are allowed: %+v", r)
		}
	}
	// but it cannot be seen before it was even sent
	early := []Op{{Client: 1, Kind: "put", Key: "x", Value: "a", Call: 100, Unknown: true},
		{Client: 2, Kind: "get", Key: "x", Output: "a", Call: 10, Return: 20}}
	if r := Check(early, 0); r.OK {
		t.Fatal("a value read before its write was sent must fail")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	h := []Op{
		{Client: 1, Kind: "put", Key: "x", Value: "a", Output: "a", Call: 0, Return: 10},
		{Client: 1, Kind: "put", Key: "y", Value: "b", Output: "b", Call: 11, Return: 20},
		{Client: 2, Kind: "get", Key: "x", Output: "a", Call: 30, Return: 35},
		{Client: 2, Kind: "get", Key: "y", Output: "b", Call: 40, Return: 45},
	}
	if r := Check(h, 0); !r.OK || r.Keys != 2 {
		t.Fatalf("%+v", r)
	}
}
