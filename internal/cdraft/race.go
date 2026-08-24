package cdraft

import (
	"context"
	"time"
)

// lateConsistencyGrace bounds how long the loser path is given to arrive for the
// post-decision SameDecision check, independent of the caller's request context.
// A real client (e.g. the CLI) cancels its request ctx the instant Write returns
// with a winner; tying the drain to that ctx would discard the slightly-later
// response and silently disable the cross-check. This detached budget keeps the
// check alive long enough to observe the loser without leaking forever.
const lateConsistencyGrace = 2 * time.Second

type Response struct {
	Result ClientResult
	Err    error
}

type RaceDecision struct {
	Result     ClientResult
	Winner     ResultSource
	LateErrors <-chan error
}

// RaceUnaryWrite starts the normal unary RPC independently, so winning through
// Fast Return never cancels or suppresses the Global Leader's normal response.
func RaceUnaryWrite(
	ctx context.Context,
	requestID string,
	unaryGlobalCall func() (ClientResult, error),
	fast <-chan Response,
) (RaceDecision, error) {
	global := make(chan Response, 1)
	go func() {
		result, err := unaryGlobalCall()
		global <- Response{Result: result, Err: err}
		close(global)
	}()
	return RaceResponses(ctx, requestID, global, fast)
}

func RaceResponses(ctx context.Context, requestID string, global, fast <-chan Response) (RaceDecision, error) {
	trace := WriteTraceFromContext(ctx)
	finishRace := trace.Start(WriteTraceEvent{RequestID: requestID, Component: "client", Phase: "client.race.wait"})
	globalOpen, fastOpen := true, true
	for globalOpen || fastOpen {
		select {
		case <-ctx.Done():
			finishRace("deadline")
			return RaceDecision{}, ErrDeadline
		case response, ok := <-global:
			if !ok {
				globalOpen = false
				global = nil
				continue
			}
			if result, valid := validResponse(response, requestID, GlobalResponse); valid {
				finishRace("winner=global-leader")
				return RaceDecision{Result: result, Winner: GlobalResponse, LateErrors: startLateDrain(requestID, result, nil, fast, trace)}, nil
			}
			globalOpen = false
			global = nil
		case response, ok := <-fast:
			if !ok {
				fastOpen = false
				fast = nil
				continue
			}
			if result, valid := validResponse(response, requestID, FastResponse); valid {
				finishRace("winner=domain-leader-fast-return")
				return RaceDecision{Result: result, Winner: FastResponse, LateErrors: startLateDrain(requestID, result, global, nil, trace)}, nil
			}
			fastOpen = false
			fast = nil
		}
	}
	finishRace("both-paths-failed")
	return RaceDecision{}, ErrBothPathsFailed
}

func validResponse(response Response, requestID string, source ResultSource) (ClientResult, bool) {
	if response.Err != nil || response.Result.Source != source || response.Result.Validate(requestID) != nil {
		return ClientResult{}, false
	}
	return response.Result, true
}

// startLateDrain runs the loser-path consistency check on a detached, bounded
// context so it survives the caller cancelling its request the moment a winner
// is returned.
func startLateDrain(requestID string, winner ClientResult, global, fast <-chan Response, trace *WriteTraceRecorder) <-chan error {
	late := make(chan error, 2)
	go func() {
		finish := trace.Start(WriteTraceEvent{RequestID: requestID, Component: "client", Phase: "client.late_drain"})
		defer finish()
		ctx, cancel := context.WithTimeout(context.Background(), lateConsistencyGrace)
		defer cancel()
		drainLate(ctx, requestID, winner, global, fast, late)
	}()
	return late
}

func drainLate(ctx context.Context, requestID string, winner ClientResult, global, fast <-chan Response, errors chan<- error) {
	defer close(errors)
	for global != nil || fast != nil {
		select {
		case <-ctx.Done():
			return
		case response, ok := <-global:
			if !ok {
				global = nil
				continue
			}
			if result, valid := validResponse(response, requestID, GlobalResponse); valid && !winner.SameDecision(result) {
				errors <- ErrInconsistentResult
			}
			global = nil
		case response, ok := <-fast:
			if !ok {
				fast = nil
				continue
			}
			if result, valid := validResponse(response, requestID, FastResponse); valid && !winner.SameDecision(result) {
				errors <- ErrInconsistentResult
			}
			fast = nil
		}
	}
}
