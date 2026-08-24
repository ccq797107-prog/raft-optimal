package cdraft

import (
	"sync"
	"time"
)

// This file holds the raw telemetry the node MEASURES: a sliding per-domain
// read/write window and a directed one-way latency matrix. The node only
// collects and exposes these signals (via the Metrics RPC). The paper cost
// model that turns them into a "best Global Leader domain" decision lives in the
// external controller (internal/optimizer + cmd/cdraft-mover), not here.

// requestEvent is one observed client request used for the sliding window.
type requestEvent struct {
	at     time.Time
	domain string
	isRead bool
}

// statsWindow accumulates per-domain write/read counts over a sliding time
// window. It is fed by the Global Leader's real Write/Read handlers.
type statsWindow struct {
	mu     sync.Mutex
	window time.Duration
	events []requestEvent
}

func newStatsWindow(window time.Duration) *statsWindow {
	return &statsWindow{window: window}
}

func (s *statsWindow) record(domain string, isRead bool, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, requestEvent{at: now, domain: domain, isRead: isRead})
	s.pruneLocked(now)
}

func (s *statsWindow) pruneLocked(now time.Time) {
	cutoff := now.Add(-s.window)
	keep := s.events[:0]
	for _, event := range s.events {
		if event.at.After(cutoff) {
			keep = append(keep, event)
		}
	}
	s.events = append([]requestEvent(nil), keep...)
}

func (s *statsWindow) counts(now time.Time) (writes, reads map[string]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	writes = make(map[string]float64)
	reads = make(map[string]float64)
	for _, event := range s.events {
		if event.isRead {
			reads[event.domain]++
		} else {
			writes[event.domain]++
		}
	}
	return writes, reads
}

// rttStore holds directed one-way latency estimates measured by real Ping
// round trips. Domain Leaders measure RTT and store one-way = RTT/2.
type rttStore struct {
	mu     sync.Mutex
	oneWay map[string]map[string]float64
}

func newRTTStore() *rttStore {
	return &rttStore{oneWay: make(map[string]map[string]float64)}
}

func (r *rttStore) observeRTT(from, to string, rtt time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.oneWay[from] == nil {
		r.oneWay[from] = make(map[string]float64)
	}
	oneWayMillis := float64(rtt.Milliseconds()) / 2
	// Exponential smoothing keeps the estimate stable under jitter.
	if prev, ok := r.oneWay[from][to]; ok {
		r.oneWay[from][to] = 0.7*prev + 0.3*oneWayMillis
	} else {
		r.oneWay[from][to] = oneWayMillis
	}
}

func (r *rttStore) merge(from string, perDomain map[string]float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.oneWay[from] == nil {
		r.oneWay[from] = make(map[string]float64)
	}
	for to, v := range perDomain {
		r.oneWay[from][to] = v
	}
}

func (r *rttStore) snapshot() map[string]map[string]float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]map[string]float64, len(r.oneWay))
	for from, row := range r.oneWay {
		copied := make(map[string]float64, len(row))
		for to, v := range row {
			copied[to] = v
		}
		out[from] = copied
	}
	return out
}

func (r *rttStore) localSnapshot(from string) map[string]float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]float64)
	for to, v := range r.oneWay[from] {
		out[to] = v
	}
	return out
}

func (r *rttStore) lookupOneWay(from, to string) (float64, bool) {
	if from == to {
		return 0, true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.oneWay[from]
	if row == nil {
		return 0, false
	}
	v, ok := row[to]
	return v, ok
}

type floatingLatencyEstimate struct {
	oneWayMillis float64
	observedAt   time.Time
}

type floatingLatencyStore struct {
	mu     sync.Mutex
	ttl    time.Duration
	values map[string]map[string]floatingLatencyEstimate
}

func newFloatingLatencyStore(ttl time.Duration) *floatingLatencyStore {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	return &floatingLatencyStore{ttl: ttl, values: make(map[string]map[string]floatingLatencyEstimate)}
}

func (s *floatingLatencyStore) observe(origin, to string, oneWayMillis float64, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.values[origin] == nil {
		s.values[origin] = make(map[string]floatingLatencyEstimate)
	}
	if prev, ok := s.values[origin][to]; ok {
		oneWayMillis = 0.7*prev.oneWayMillis + 0.3*oneWayMillis
	}
	s.values[origin][to] = floatingLatencyEstimate{oneWayMillis: oneWayMillis, observedAt: now}
}

func (s *floatingLatencyStore) oneWay(origin, to string, now time.Time) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.values[origin]
	if row == nil {
		return 0, false
	}
	estimate, ok := row[to]
	if !ok || now.Sub(estimate.observedAt) > s.ttl {
		return 0, false
	}
	return estimate.oneWayMillis, true
}

func (s *floatingLatencyStore) freshSnapshot(now time.Time) map[string]map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]map[string]float64)
	for origin, row := range s.values {
		for to, estimate := range row {
			if now.Sub(estimate.observedAt) > s.ttl {
				continue
			}
			if out[origin] == nil {
				out[origin] = make(map[string]float64)
			}
			out[origin][to] = estimate.oneWayMillis
		}
	}
	return out
}

func (s *floatingLatencyStore) ageMillis(now time.Time) map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]uint64)
	for origin, row := range s.values {
		for to, estimate := range row {
			if now.Sub(estimate.observedAt) > s.ttl {
				continue
			}
			out[origin+"->"+to] = uint64(now.Sub(estimate.observedAt).Milliseconds())
		}
	}
	return out
}

func (s *floatingLatencyStore) ttlMillis() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(s.ttl.Milliseconds())
}
