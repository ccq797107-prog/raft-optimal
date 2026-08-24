package cdraft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
)

// KVEngine is the minimal, storage-engine-agnostic key/value primitive the
// consensus layer's persistence is built on. Any embedded ordered KV store
// (LevelDB, RocksDB, BoltDB, Pebble, ...) can back it by implementing this
// handful of methods; the upper layers never reference a concrete engine.
//
// Contract:
//   - Keys are ordered: Scan MUST visit keys with the given prefix in ascending
//     lexicographic byte order. (KVStore relies on this to read the log back in
//     GlobalIndex order via big-endian index keys.)
//   - Write applies all ops atomically (all-or-nothing), so a single Save lands
//     as one consistent unit.
//   - In Scan's callback the key/value slices are only valid for the duration of
//     the call; the callback must copy anything it needs to retain.
type KVEngine interface {
	Get(key []byte) (value []byte, found bool, err error)
	Write(ops []KVOp) error
	Scan(prefix []byte, fn func(key, value []byte) error) error
	Close() error
}

// KVOp is one entry in an atomic batch. Delete removes Key (Value ignored),
// otherwise it is a Put of Key=Value.
type KVOp struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// KVStore implements Store on top of any KVEngine. It owns the on-disk layout
// (and the incremental-write bookkeeping) so the byte encoding is identical no
// matter which engine is plugged in underneath:
//
//   - "meta"            : one JSON document with the small Raft metadata
//     (terms, votes, leader identities, per-domain quorum indices, the global
//     commit / applied indices).
//   - "log:<index>"     : one JSON-encoded LogEntry per replicated entry, keyed
//     by an 8-byte big-endian GlobalIndex so a prefix scan yields the log in
//     order. Save diffs the in-memory log against a per-index fingerprint of
//     what is already durable and writes only the entries that were appended OR
//     overwritten (a conflicting uncommitted tail can be truncated + replaced by
//     appendEntryChecked), deleting any keys past a shrunken tail.
//   - "sm:<key>"        : the applied state machine, one key per applied
//     key/value pair. Save flushes only the keys whose value changed.
//   - "res:<requestId>" : the idempotency result cache, one JSON-encoded
//     ClientResult per requestId. Like the state machine it only grows, so Save
//     flushes only the newly settled requests.
//
// Every Save commits its meta + log-tail + state-machine delta + result delta as
// one atomic engine batch, so persisted state is always internally consistent.
type KVStore struct {
	mu       sync.Mutex
	engine   KVEngine
	lastMeta []byte
	// lastLog / lastSM / lastRes mirror what is already durable so each Save
	// writes only the delta instead of rewriting the entire log, key space and
	// result cache. lastLog holds a cheap fingerprint per GlobalIndex so an
	// in-place overwrite is detected and flushed, and any index no longer in
	// memory (tail truncation OR snapshot compaction of the prefix) is deleted.
	lastLog map[uint64]logFingerprint
	lastSM  map[string]string
	lastRes map[string]ClientResult
}

// logFingerprint is the minimal identity of a persisted log slot, enough to tell
// whether the entry now in memory at this index differs from what is on disk.
type logFingerprint struct {
	index uint64
	term  uint64
	req   string
}

func fingerprintOf(entry LogEntry) logFingerprint {
	return logFingerprint{index: entry.GlobalIndex, term: entry.GlobalTerm, req: entry.RequestID}
}

const (
	metaKey     = "meta"
	logPrefix   = "log:"
	smKeyPrefix = "sm:"
	resPrefix   = "res:"
)

// NewKVStore wraps a KVEngine as a consensus-facing Store.
func NewKVStore(engine KVEngine) *KVStore {
	return &KVStore{
		engine:  engine,
		lastLog: make(map[uint64]logFingerprint),
		lastSM:  make(map[string]string),
		lastRes: make(map[string]ClientResult),
	}
}

func (s *KVStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.engine == nil {
		return nil
	}
	err := s.engine.Close()
	s.engine = nil
	return err
}

func logKey(index uint64) []byte {
	key := make([]byte, len(logPrefix)+8)
	copy(key, logPrefix)
	binary.BigEndian.PutUint64(key[len(logPrefix):], index)
	return key
}

func (s *KVStore) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := PersistentState{}
	if raw, found, err := s.engine.Get([]byte(metaKey)); err != nil {
		return PersistentState{}, fmt.Errorf("read meta: %w", err)
	} else if found {
		if err := json.Unmarshal(raw, &state); err != nil {
			return PersistentState{}, fmt.Errorf("decode meta: %w", err)
		}
		s.lastMeta = append(s.lastMeta[:0], raw...)
	}

	// Rebuild the log in index order from the "log:" key space.
	log := make([]LogEntry, 0)
	if err := s.engine.Scan([]byte(logPrefix), func(_, value []byte) error {
		var entry LogEntry
		if err := json.Unmarshal(value, &entry); err != nil {
			return fmt.Errorf("decode log entry: %w", err)
		}
		log = append(log, entry)
		return nil
	}); err != nil {
		return PersistentState{}, fmt.Errorf("scan log: %w", err)
	}
	state.Log = log

	// Rebuild the applied key/value map from the "sm:" key space.
	sm := make(map[string]string)
	if err := s.engine.Scan([]byte(smKeyPrefix), func(key, value []byte) error {
		sm[string(key[len(smKeyPrefix):])] = string(value)
		return nil
	}); err != nil {
		return PersistentState{}, fmt.Errorf("scan state machine: %w", err)
	}
	state.StateMachine = sm

	// Rebuild the idempotency result cache from the "res:" key space.
	results := make(map[string]ClientResult)
	if err := s.engine.Scan([]byte(resPrefix), func(key, value []byte) error {
		var res ClientResult
		if err := json.Unmarshal(value, &res); err != nil {
			return fmt.Errorf("decode result: %w", err)
		}
		results[string(key[len(resPrefix):])] = res
		return nil
	}); err != nil {
		return PersistentState{}, fmt.Errorf("scan results: %w", err)
	}
	state.Results = results

	state.initMaps()

	s.lastLog = make(map[uint64]logFingerprint, len(log))
	for i := range log {
		s.lastLog[log[i].GlobalIndex] = fingerprintOf(log[i])
	}
	s.lastSM = make(map[string]string, len(sm))
	for k, v := range sm {
		s.lastSM[k] = v
	}
	s.lastRes = make(map[string]ClientResult, len(results))
	for k, v := range results {
		s.lastRes[k] = v
	}
	return state, nil
}

func (s *KVStore) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state.initMaps()

	ops := make([]KVOp, 0, 1+len(state.Log))

	// Meta: the small Raft layer, serialised without the log, key space and
	// result cache which have their own (incrementally written) key prefixes.
	meta := state
	meta.Log = nil
	meta.StateMachine = nil
	meta.Results = nil
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if !bytes.Equal(s.lastMeta, metaJSON) {
		ops = append(ops, KVOp{Key: []byte(metaKey), Value: metaJSON})
	}

	// Log: flush appended AND overwritten entries, and delete every durable
	// index that is no longer held in memory — whether the tail shrank (conflict
	// truncation) or the prefix was compacted into a snapshot. Diffing by
	// GlobalIndex (not slice position) keeps the bookkeeping correct once the
	// in-memory log no longer starts at index 1.
	desired := make(map[uint64]logFingerprint, len(state.Log))
	for i := range state.Log {
		desired[state.Log[i].GlobalIndex] = fingerprintOf(state.Log[i])
	}
	for index := range s.lastLog {
		if _, held := desired[index]; !held {
			ops = append(ops, KVOp{Key: logKey(index), Delete: true})
		}
	}
	for i := range state.Log {
		if prev, ok := s.lastLog[state.Log[i].GlobalIndex]; ok && prev == fingerprintOf(state.Log[i]) {
			continue // unchanged slot, already durable
		}
		entryJSON, err := json.Marshal(state.Log[i])
		if err != nil {
			return err
		}
		ops = append(ops, KVOp{Key: logKey(state.Log[i].GlobalIndex), Value: entryJSON})
	}

	// State machine: flush only the changed keys.
	smChanged := false
	for k, v := range state.StateMachine {
		if prev, ok := s.lastSM[k]; !ok || prev != v {
			ops = append(ops, KVOp{Key: []byte(smKeyPrefix + k), Value: []byte(v)})
			smChanged = true
		}
	}

	// Result cache: flush only the newly settled / changed requests.
	resChanged := false
	for k, v := range state.Results {
		if prev, ok := s.lastRes[k]; !ok || prev != v {
			resJSON, err := json.Marshal(v)
			if err != nil {
				return err
			}
			ops = append(ops, KVOp{Key: []byte(resPrefix + k), Value: resJSON})
			resChanged = true
		}
	}

	if len(ops) == 0 {
		return nil
	}
	if err := s.engine.Write(ops); err != nil {
		return fmt.Errorf("write store: %w", err)
	}

	s.lastMeta = append(s.lastMeta[:0], metaJSON...)
	s.lastLog = desired
	if smChanged {
		for k, v := range state.StateMachine {
			s.lastSM[k] = v
		}
	}
	if resChanged {
		for k, v := range state.Results {
			s.lastRes[k] = v
		}
	}
	return nil
}
