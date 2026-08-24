package cdraft

import (
	"context"
	"sort"
	"sync"
	"time"
)

type writeTraceContextKey struct{}

// WriteTraceEvent is one timed segment of a client write. Start/End are kept so
// reports can preserve parallelism instead of pretending all phases are serial.
type WriteTraceEvent struct {
	RequestID  string
	Component  string
	Phase      string
	NodeID     string
	DomainID   string
	PeerDomain string
	Detail     string
	StartedAt  time.Time
	EndedAt    time.Time
	Duration   time.Duration
}

// WriteTraceRecorder is intentionally internal and opt-in. Normal clients do
// not receive or depend on trace fields.
type WriteTraceRecorder struct {
	mu     sync.Mutex
	events []WriteTraceEvent
}

func NewWriteTraceRecorder() *WriteTraceRecorder {
	return &WriteTraceRecorder{}
}

func WithWriteTrace(ctx context.Context, recorder *WriteTraceRecorder) context.Context {
	if recorder == nil {
		return ctx
	}
	return context.WithValue(ctx, writeTraceContextKey{}, recorder)
}

func WriteTraceFromContext(ctx context.Context) *WriteTraceRecorder {
	if ctx == nil {
		return nil
	}
	recorder, _ := ctx.Value(writeTraceContextKey{}).(*WriteTraceRecorder)
	return recorder
}

func (r *WriteTraceRecorder) Start(event WriteTraceEvent) func(detail ...string) {
	if r == nil {
		return func(...string) {}
	}
	if event.StartedAt.IsZero() {
		event.StartedAt = time.Now()
	}
	return func(detail ...string) {
		event.EndedAt = time.Now()
		event.Duration = event.EndedAt.Sub(event.StartedAt)
		if len(detail) > 0 {
			event.Detail = detail[0]
		}
		r.Record(event)
	}
}

func (r *WriteTraceRecorder) Mark(event WriteTraceEvent) {
	if r == nil {
		return
	}
	now := time.Now()
	if event.StartedAt.IsZero() {
		event.StartedAt = now
	}
	if event.EndedAt.IsZero() {
		event.EndedAt = now
	}
	event.Duration = event.EndedAt.Sub(event.StartedAt)
	r.Record(event)
}

func (r *WriteTraceRecorder) Record(event WriteTraceEvent) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *WriteTraceRecorder) Events() []WriteTraceEvent {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	events := append([]WriteTraceEvent(nil), r.events...)
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].StartedAt.Equal(events[j].StartedAt) {
			return events[i].Phase < events[j].Phase
		}
		return events[i].StartedAt.Before(events[j].StartedAt)
	})
	return events
}
