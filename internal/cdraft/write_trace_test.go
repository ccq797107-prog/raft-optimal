package cdraft

import (
	"testing"
	"time"
)

func TestWriteTraceRecorderKeepsTimedParallelEvents(t *testing.T) {
	recorder := NewWriteTraceRecorder()
	base := time.Now()
	recorder.Record(WriteTraceEvent{
		RequestID: "r1", Component: "server", Phase: "server.domain_quorum",
		DomainID: "c", StartedAt: base.Add(10 * time.Millisecond), EndedAt: base.Add(30 * time.Millisecond),
		Duration: 20 * time.Millisecond,
	})
	recorder.Record(WriteTraceEvent{
		RequestID: "r1", Component: "server", Phase: "server.domain_quorum",
		DomainID: "a", StartedAt: base, EndedAt: base.Add(25 * time.Millisecond),
		Duration: 25 * time.Millisecond,
	})

	events := recorder.Events()
	if len(events) != 2 {
		t.Fatalf("events=%d, want 2", len(events))
	}
	if events[0].DomainID != "a" || events[1].DomainID != "c" {
		t.Fatalf("events not sorted by start time: %+v", events)
	}
	if events[0].Duration+events[1].Duration == events[1].EndedAt.Sub(events[0].StartedAt) {
		t.Fatalf("trace collapsed parallel phases into a serial duration: %+v", events)
	}
}
