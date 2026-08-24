package cdraft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func result(source ResultSource, value string) ClientResult {
	return ClientResult{
		RequestID: "r1", GlobalTerm: 2, GlobalIndex: 3, Result: value, Source: source, Committed: true,
	}
}

func responseChannel(responses ...Response) <-chan Response {
	channel := make(chan Response, len(responses))
	for _, response := range responses {
		channel <- response
	}
	close(channel)
	return channel
}

func TestResponseRaceAcceptsFirstLegalSuccessAndConsumesLateResult(t *testing.T) {
	tests := []struct {
		name       string
		global     func() <-chan Response
		fast       func() <-chan Response
		wantWinner ResultSource
	}{
		{
			name:       "fast return first",
			global:     func() <-chan Response { return delayedResponse(Response{Result: result(GlobalResponse, "x=1")}) },
			fast:       func() <-chan Response { return responseChannel(Response{Result: result(FastResponse, "x=1")}) },
			wantWinner: FastResponse,
		},
		{
			name:       "global response first",
			global:     func() <-chan Response { return responseChannel(Response{Result: result(GlobalResponse, "x=1")}) },
			fast:       func() <-chan Response { return delayedResponse(Response{Result: result(FastResponse, "x=1")}) },
			wantWinner: GlobalResponse,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			decision, err := RaceResponses(ctx, "r1", test.global(), test.fast())
			if err != nil || decision.Winner != test.wantWinner {
				t.Fatalf("unexpected decision: %+v err=%v", decision, err)
			}
			for lateErr := range decision.LateErrors {
				t.Fatalf("matching late response failed validation: %v", lateErr)
			}
		})
	}
}

func TestResponseRaceDoesNotLetOnePathErrorTerminateTheOther(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	decision, err := RaceResponses(
		ctx,
		"r1",
		responseChannel(Response{Err: errors.New("transient gRPC error")}),
		responseChannel(Response{Result: result(FastResponse, "x=1")}),
	)
	if err != nil || decision.Winner != FastResponse {
		t.Fatalf("transient global error terminated Fast Return: %+v err=%v", decision, err)
	}
}

func TestAsyncUnaryContinuesAfterFastReturnWins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	unaryCompleted := make(chan struct{})
	decision, err := RaceUnaryWrite(
		ctx,
		"r1",
		func() (ClientResult, error) {
			time.Sleep(10 * time.Millisecond)
			close(unaryCompleted)
			return result(GlobalResponse, "x=1"), nil
		},
		responseChannel(Response{Result: result(FastResponse, "x=1")}),
	)
	if err != nil || decision.Winner != FastResponse {
		t.Fatalf("Fast Return did not win: %+v err=%v", decision, err)
	}
	select {
	case <-unaryCompleted:
	case <-ctx.Done():
		t.Fatal("normal unary RPC was canceled after Fast Return")
	}
	for lateErr := range decision.LateErrors {
		t.Fatalf("normal unary late result failed validation: %v", lateErr)
	}
}

func TestMissingOrInvalidFastReturnFallsBackToGlobalResponse(t *testing.T) {
	invalid := result(FastResponse, "x=1")
	invalid.GlobalTerm = 0
	tests := []struct {
		name string
		fast <-chan Response
	}{
		{name: "missing", fast: responseChannel()},
		{name: "invalid", fast: responseChannel(Response{Result: invalid})},
		{name: "transient error", fast: responseChannel(Response{Err: errors.New("fast callback unavailable")})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			decision, err := RaceResponses(
				ctx,
				"r1",
				delayedResponse(Response{Result: result(GlobalResponse, "x=1")}),
				test.fast,
			)
			if err != nil || decision.Winner != GlobalResponse {
				t.Fatalf("normal response fallback failed: %+v err=%v", decision, err)
			}
		})
	}
}

func TestResponseRaceRejectsInvalidAndReportsInconsistentLateSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	decision, err := RaceResponses(
		ctx,
		"r1",
		responseChannel(Response{Result: result(GlobalResponse, "x=1")}),
		delayedResponse(Response{Result: result(FastResponse, "x=DIFFERENT")}),
	)
	if err != nil {
		t.Fatal(err)
	}
	var got error
	for lateErr := range decision.LateErrors {
		got = lateErr
	}
	if !errors.Is(got, ErrInconsistentResult) {
		t.Fatalf("late inconsistency was not reported: %v", got)
	}

	invalid := result(FastResponse, "x=1")
	invalid.RequestID = "wrong"
	_, err = RaceResponses(ctx, "r1", responseChannel(Response{Err: errors.New("failed")}), responseChannel(Response{Result: invalid}))
	if !errors.Is(err, ErrBothPathsFailed) {
		t.Fatalf("invalid result was accepted: %v", err)
	}
}

func TestResponseRaceDeadlineAndNearSimultaneousCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RaceResponses(ctx, "r1", make(chan Response), make(chan Response)); !errors.Is(err, ErrDeadline) {
		t.Fatalf("deadline semantics changed: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	decision, err := RaceResponses(
		ctx,
		"r1",
		responseChannel(Response{Result: result(GlobalResponse, "x=1")}),
		responseChannel(Response{Result: result(FastResponse, "x=1")}),
	)
	if err != nil || (decision.Winner != GlobalResponse && decision.Winner != FastResponse) {
		t.Fatalf("near-simultaneous successes did not complete once: %+v err=%v", decision, err)
	}
	for lateErr := range decision.LateErrors {
		t.Fatalf("matching duplicate caused a second completion: %v", lateErr)
	}
}

func delayedResponse(response Response) <-chan Response {
	channel := make(chan Response, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		channel <- response
		close(channel)
	}()
	return channel
}
