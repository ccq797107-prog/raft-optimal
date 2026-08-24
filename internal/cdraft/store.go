package cdraft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type PersistentState struct {
	DomainTerm             uint64                  `json:"domainTerm"`
	DomainVotedFor         string                  `json:"domainVotedFor"`
	DomainLeader           DomainLeaderIdentity    `json:"domainLeader"`
	GlobalTerm             uint64                  `json:"globalTerm"`
	GlobalVotedFor         DomainLeaderIdentity    `json:"globalVotedFor"`
	GlobalLeader           GlobalLeaderIdentity    `json:"globalLeader"`
	Log                    []LogEntry              `json:"log"`
	DomainQuorumIndex      map[string]uint64       `json:"domainQuorumIndex"`
	KnownGlobalCommitIndex uint64                  `json:"knownGlobalCommitIndex"`
	AppliedIndex           uint64                  `json:"appliedIndex"`
	Results                map[string]ClientResult `json:"results"`
	StateMachine           map[string]string       `json:"stateMachine"`

	// Log compaction (snapshot) metadata. Entries 1..SnapshotLastIndex have been
	// discarded from Log; their effects live in StateMachine/Results, and the
	// identity of the boundary entry (term + requestId) is retained so
	// log-matching and commit-identity checks keep working across the cut.
	// Log, when non-empty, holds the contiguous entries
	// [SnapshotLastIndex+1 .. SnapshotLastIndex+len(Log)].
	SnapshotLastIndex     uint64 `json:"snapshotLastIndex,omitempty"`
	SnapshotLastTerm      uint64 `json:"snapshotLastTerm,omitempty"`
	SnapshotLastRequestID string `json:"snapshotLastRequestId,omitempty"`
}

func (s PersistentState) Clone() PersistentState {
	data, _ := json.Marshal(s)
	var clone PersistentState
	_ = json.Unmarshal(data, &clone)
	clone.initMaps()
	return clone
}

func (s *PersistentState) initMaps() {
	if s.DomainQuorumIndex == nil {
		s.DomainQuorumIndex = make(map[string]uint64)
	}
	if s.Results == nil {
		s.Results = make(map[string]ClientResult)
	}
	if s.StateMachine == nil {
		s.StateMachine = make(map[string]string)
	}
}

func (s PersistentState) Summary() LogSummary {
	if len(s.Log) == 0 {
		// A fully compacted log is still "as up to date as" the entries the
		// snapshot covers, so elections compare against the snapshot boundary.
		return LogSummary{LastGlobalTerm: s.SnapshotLastTerm, LastGlobalIndex: s.SnapshotLastIndex}
	}
	last := s.Log[len(s.Log)-1]
	return LogSummary{LastGlobalTerm: last.GlobalTerm, LastGlobalIndex: last.GlobalIndex}
}

// LastLogIndex is the index of the newest entry this state covers, whether it
// still sits in Log or was compacted into the snapshot.
func (s PersistentState) LastLogIndex() uint64 {
	return s.SnapshotLastIndex + uint64(len(s.Log))
}

type Store interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

type MemoryStore struct {
	mu    sync.Mutex
	state PersistentState
}

func NewMemoryStore() *MemoryStore {
	state := PersistentState{}
	state.initMaps()
	return &MemoryStore{state: state}
}

func (s *MemoryStore) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Clone(), nil
}

func (s *MemoryStore) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state.Clone()
	return nil
}

type FileStore struct {
	mu   sync.Mutex
	path string
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (s *FileStore) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		state := PersistentState{}
		state.initMaps()
		return state, nil
	}
	if err != nil {
		return PersistentState{}, err
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, fmt.Errorf("decode state: %w", err)
	}
	state.initMaps()
	return state, nil
}

func (s *FileStore) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state.initMaps()
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
