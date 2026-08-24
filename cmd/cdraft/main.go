package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/czq/cd-raft/internal/cdraft"
	"github.com/czq/cd-raft/internal/topology"
)

func main() {
	configPath := flag.String("config", "config/cluster.json", "static CD-Raft topology")
	nodeID := flag.String("node", "", "local node id")
	dataDir := flag.String("data", "data", "persistent node state directory")
	flag.Parse()

	cfg, err := topology.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	node, ok := cfg.Node(*nodeID)
	if !ok {
		log.Fatalf("unknown node %q", *nodeID)
	}
	store, err := cdraft.NewLevelDBStore(*dataDir + "/" + *nodeID)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	runtime, err := cdraft.NewRPCNode(cfg, *nodeID, store)
	if err != nil {
		log.Fatal(err)
	}
	if err := runtime.Start(); err != nil {
		log.Fatal(err)
	}
	log.Printf("node=%s domain=%s client(bind=%s advertise=%s) inter-domain(bind=%s advertise=%s)",
		node.ID, node.DomainCode,
		node.ClientBindAddress(), node.ListenAddress,
		node.InterDomainBindAddress(), node.InterDomainDialAddress())
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	runtime.Run(ctx)
}
