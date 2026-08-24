package cdraft

import (
	"path/filepath"
	"testing"
)

func TestFileStorePersistsTermsVotesAndLog(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "node.json"))
	state := PersistentState{
		DomainTerm:     7,
		DomainVotedFor: "a2",
		GlobalTerm:     11,
		GlobalVotedFor: DomainLeaderIdentity{DomainID: "a", NodeID: "a2", DomainTerm: 7},
		Log: []LogEntry{{
			GlobalTerm: 11, GlobalIndex: 1, RequestID: "r1",
		}},
	}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if recovered.DomainTerm != 7 || recovered.DomainVotedFor != "a2" ||
		recovered.GlobalTerm != 11 || recovered.GlobalVotedFor.NodeID != "a2" ||
		recovered.Summary().LastGlobalIndex != 1 {
		t.Fatalf("state did not survive restart: %+v", recovered)
	}
}

func TestElectionRPCPolicyHasDeadlineAndBoundedRetries(t *testing.T) {
	policy := DefaultRPCPolicy()
	if policy.Deadline <= 0 || policy.MaxRetries <= 0 {
		t.Fatalf("unsafe RPC policy: %+v", policy)
	}
}
