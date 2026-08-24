package cdraft

import (
	"bytes"
	"path/filepath"
	"sort"
	"testing"
)

// memKVEngine is a tiny in-memory KVEngine used to prove KVStore is fully
// engine-agnostic (it backs the store with no LevelDB at all). It also serves as
// a reference for writing other adapters (RocksDB, BoltDB, ...).
type memKVEngine struct {
	data   map[string][]byte
	writes int
}

func newMemKVEngine() *memKVEngine { return &memKVEngine{data: make(map[string][]byte)} }

func (e *memKVEngine) Get(key []byte) ([]byte, bool, error) {
	v, ok := e.data[string(key)]
	return v, ok, nil
}

func (e *memKVEngine) Write(ops []KVOp) error {
	e.writes++
	for _, op := range ops {
		if op.Delete {
			delete(e.data, string(op.Key))
		} else {
			e.data[string(op.Key)] = append([]byte(nil), op.Value...)
		}
	}
	return nil
}

func (e *memKVEngine) Scan(prefix []byte, fn func(key, value []byte) error) error {
	keys := make([]string, 0, len(e.data))
	for k := range e.data {
		if bytes.HasPrefix([]byte(k), prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys) // KVEngine contract: ascending key order
	for _, k := range keys {
		if err := fn([]byte(k), e.data[k]); err != nil {
			return err
		}
	}
	return nil
}

func (e *memKVEngine) Close() error { return nil }

func TestKVStoreIsEngineAgnostic(t *testing.T) {
	store := NewKVStore(newMemKVEngine())
	state := PersistentState{
		GlobalTerm: 5,
		Log: []LogEntry{
			{GlobalTerm: 5, GlobalIndex: 1, RequestID: "r1"},
			{GlobalTerm: 5, GlobalIndex: 2, RequestID: "r2"},
		},
		AppliedIndex: 2,
		StateMachine: map[string]string{"k": "v"},
		Results:      map[string]ClientResult{"r1": {RequestID: "r1", GlobalTerm: 5, GlobalIndex: 1, Committed: true}},
	}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Log) != 2 || got.Log[1].GlobalIndex != 2 || got.StateMachine["k"] != "v" ||
		got.AppliedIndex != 2 || !got.Results["r1"].Committed || got.GlobalTerm != 5 {
		t.Fatalf("round trip over a non-leveldb engine failed: %+v", got)
	}
}

func TestKVStoreSkipsNoOpSave(t *testing.T) {
	engine := newMemKVEngine()
	store := NewKVStore(engine)
	state := PersistentState{
		GlobalTerm:   1,
		StateMachine: map[string]string{"k": "v"},
		Results:      map[string]ClientResult{},
	}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	if engine.writes != 1 {
		t.Fatalf("writes after first save = %d, want 1", engine.writes)
	}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	if engine.writes != 1 {
		t.Fatalf("no-op save wrote to engine: writes=%d, want 1", engine.writes)
	}
	state.GlobalTerm = 2
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	if engine.writes != 2 {
		t.Fatalf("changed state did not write to engine: writes=%d, want 2", engine.writes)
	}
}

func TestLevelDBStorePersistsRaftLogAndStateMachine(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	store, err := NewLevelDBStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	state := PersistentState{
		DomainTerm:     7,
		DomainVotedFor: "a2",
		GlobalTerm:     11,
		GlobalVotedFor: DomainLeaderIdentity{DomainID: "a", NodeID: "a2", DomainTerm: 7},
		Log: []LogEntry{
			{GlobalTerm: 11, GlobalIndex: 1, RequestID: "r1"},
			{GlobalTerm: 11, GlobalIndex: 2, RequestID: "r2"},
		},
		AppliedIndex: 2,
		StateMachine: map[string]string{"alpha": "1", "beta": "2"},
		Results: map[string]ClientResult{
			"r1": {RequestID: "r1", GlobalTerm: 11, GlobalIndex: 1, Result: "ok", Committed: true},
		},
	}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}

	recovered, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := recovered.Results["r1"]; got.RequestID != "r1" || got.GlobalIndex != 1 || got.Result != "ok" || !got.Committed {
		t.Fatalf("result cache did not survive: %+v", recovered.Results)
	}
	if recovered.DomainTerm != 7 || recovered.DomainVotedFor != "a2" ||
		recovered.GlobalTerm != 11 || recovered.GlobalVotedFor.NodeID != "a2" {
		t.Fatalf("raft metadata did not survive: %+v", recovered)
	}
	if len(recovered.Log) != 2 || recovered.Log[0].GlobalIndex != 1 || recovered.Log[1].GlobalIndex != 2 ||
		recovered.Summary().LastGlobalIndex != 2 {
		t.Fatalf("log did not survive in order: %+v", recovered.Log)
	}
	if recovered.AppliedIndex != 2 ||
		recovered.StateMachine["alpha"] != "1" || recovered.StateMachine["beta"] != "2" {
		t.Fatalf("state machine did not survive: %+v", recovered)
	}
}

func TestLevelDBStoreAppendsIncrementallyAndReopens(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	store, err := NewLevelDBStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Save(PersistentState{
		Log:          []LogEntry{{GlobalTerm: 1, GlobalIndex: 1, RequestID: "r1"}},
		AppliedIndex: 1,
		StateMachine: map[string]string{"k": "v1"},
	}); err != nil {
		t.Fatal(err)
	}
	// Append a new entry and mutate an existing key.
	if err := store.Save(PersistentState{
		Log: []LogEntry{
			{GlobalTerm: 1, GlobalIndex: 1, RequestID: "r1"},
			{GlobalTerm: 1, GlobalIndex: 2, RequestID: "r2"},
		},
		AppliedIndex: 2,
		StateMachine: map[string]string{"k": "v2", "k2": "x"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewLevelDBStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered.Log) != 2 || recovered.Log[1].RequestID != "r2" {
		t.Fatalf("reopened log wrong: %+v", recovered.Log)
	}
	if recovered.StateMachine["k"] != "v2" || recovered.StateMachine["k2"] != "x" {
		t.Fatalf("reopened state machine wrong: %+v", recovered.StateMachine)
	}
	if recovered.AppliedIndex != 2 {
		t.Fatalf("reopened applied index = %d, want 2", recovered.AppliedIndex)
	}
}
