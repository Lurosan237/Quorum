// quorum-server runs one node of a Quorum cluster.
//
//	quorum-server -id 1 -peers 1=http://n1:7000,2=http://n2:7000,3=http://n3:7000 -listen :7000 -data /data
//
// Everything a node must not forget is in -data; kill it at any moment and start it again on the
// same directory, and it carries on where it was.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Lurosan237/Quorum/client"
	"github.com/Lurosan237/Quorum/server"
)

func main() {
	id := flag.Int("id", 1, "this node's id")
	peers := flag.String("peers", "", "every node, this one included: id=url,id=url,...")
	listen := flag.String("listen", ":7000", "address to serve on")
	data := flag.String("data", "data", "data directory")
	tick := flag.Duration("tick", 10*time.Millisecond, "one Raft tick (an election takes 20-40 ticks)")
	compact := flag.Uint64("compact", 1000, "snapshot every this many applied entries (0 = never)")
	noBatch := flag.Bool("no-batch", false, "one request per disk write (for benchmarks)")
	noSync := flag.Bool("no-fsync", false, "UNSAFE, for benchmarks: don't fsync the log")
	flag.Parse()

	nodes, err := client.ParseNodes(*peers)
	if err != nil {
		log.Fatal(err)
	}
	if _, ok := nodes[*id]; !ok {
		log.Fatalf("-peers must include this node (%d)", *id)
	}
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	s, err := server.New(server.Config{ID: *id, Peers: nodes, Listen: *listen, DataDir: *data, Tick: *tick,
		CompactEvery: *compact, Logger: logger, NoBatch: *noBatch, NoSync: *noSync})
	if err != nil {
		log.Fatal(err)
	}
	s.Start()
	logger.Printf("node %d serving on %s, data in %s", *id, *listen, *data)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	s.Stop()
}
