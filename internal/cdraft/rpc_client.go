package cdraft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RPCClient struct {
	mu             sync.Mutex
	conns          map[string]*grpc.ClientConn
	callbackListen string
	replyRoute     string

	// One persistent Fast Return callback server is shared by every Write so a
	// fixed callback port can be reused across many sequential writes without
	// racing on "address already in use" while the OS releases the prior bind.
	cbServer *grpc.Server
	cbRouter *callbackRouter
	cbAddr   string
	cbErr    error

	// lastLeader caches the address that last served a request (the Global
	// Leader). Subsequent requests start there directly instead of paying a
	// redirect hop (and its backoff) through a local non-leader node every time.
	lastLeader string
}

func (c *RPCClient) rememberLeader(address string) {
	c.mu.Lock()
	c.lastLeader = address
	c.mu.Unlock()
}

func (c *RPCClient) forgetLeader() {
	c.mu.Lock()
	c.lastLeader = ""
	c.mu.Unlock()
}

func (c *RPCClient) leaderOr(target string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastLeader != "" {
		return c.lastLeader
	}
	return target
}

func NewRPCClient() *RPCClient {
	return NewRPCClientWithCallback("127.0.0.1:0", "")
}

// NewRPCClientWithCallback separates the local callback bind address from the
// replyRoute advertised to Domain Leaders for cross-machine deployments.
func NewRPCClientWithCallback(callbackListen, replyRoute string) *RPCClient {
	return &RPCClient{
		conns: make(map[string]*grpc.ClientConn), callbackListen: callbackListen, replyRoute: replyRoute,
	}
}

func (c *RPCClient) Close() {
	c.mu.Lock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	c.conns = make(map[string]*grpc.ClientConn)
	server := c.cbServer
	c.mu.Unlock()
	if server != nil {
		server.GracefulStop()
	}
}

// ensureCallback lazily starts the single shared Fast Return callback server,
// returning the address to advertise and the router that dispatches incoming
// Fast Return responses to the per-request waiter.
func (c *RPCClient) ensureCallback() (string, *callbackRouter, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cbServer != nil || c.cbErr != nil {
		return c.cbAddr, c.cbRouter, c.cbErr
	}
	listener, err := net.Listen("tcp", c.callbackListen)
	if err != nil {
		c.cbErr = err
		return "", nil, err
	}
	c.cbRouter = &callbackRouter{waiters: make(map[string]chan Response), traces: make(map[string]*WriteTraceRecorder)}
	c.cbServer = grpc.NewServer()
	cdraftv1.RegisterClientCallbackServer(c.cbServer, c.cbRouter)
	c.cbAddr = listener.Addr().String()
	go func() {
		_ = c.cbServer.Serve(listener)
	}()
	return c.cbAddr, c.cbRouter, nil
}

func (c *RPCClient) Write(
	ctx context.Context,
	target string,
	request ClientWriteRequest,
) (RaceDecision, error) {
	trace := WriteTraceFromContext(ctx)
	finishWrite := trace.Start(WriteTraceEvent{RequestID: request.RequestID, Component: "client", Phase: "client.write.total"})
	defer finishWrite()
	finishCallback := trace.Start(WriteTraceEvent{RequestID: request.RequestID, Component: "client", Phase: "client.callback.ensure"})
	addr, router, err := c.ensureCallback()
	finishCallback()
	if err != nil {
		return RaceDecision{}, err
	}
	replyRoute := c.replyRoute
	if replyRoute == "" {
		replyRoute = addr
	}
	fast := router.register(request.RequestID, trace)
	trace.Mark(WriteTraceEvent{RequestID: request.RequestID, Component: "client", Phase: "client.callback.register"})

	global := make(chan Response, 1)
	go func() {
		finishGlobal := trace.Start(WriteTraceEvent{RequestID: request.RequestID, Component: "client", Phase: "client.global_rpc"})
		result, err := c.writeFollowingRedirects(ctx, target, &cdraftv1.ClientWriteRequest{
			RequestId: request.RequestID, OriginDomain: request.OriginDomain,
			ReplyRoute: replyRoute, Key: request.Command.Key, Value: request.Command.Value,
		})
		finishGlobal()
		global <- Response{Result: result, Err: err}
		close(global)
	}()
	decision, err := RaceResponses(ctx, request.RequestID, global, fast)
	if err != nil {
		router.unregister(request.RequestID)
		return RaceDecision{}, err
	}
	// The late-consistency drain (RaceDecision.LateErrors) runs on a detached,
	// bounded context, so the callback waiter must stay registered until that
	// drain finishes — NOT until the caller cancels its request (which a CLI does
	// the instant Write returns). Forward the late errors through, unregister
	// when the drain is done, and surface any cross-path inconsistency loudly so
	// a real client cannot silently ignore it.
	late := decision.LateErrors
	forwarded := make(chan error, 2)
	go func() {
		defer close(forwarded)
		defer router.unregister(request.RequestID)
		for lateErr := range late {
			if errors.Is(lateErr, ErrInconsistentResult) {
				log.Printf("cd-raft client: request %s fast/global results disagree: %v", request.RequestID, lateErr)
			}
			forwarded <- lateErr
		}
	}()
	decision.LateErrors = forwarded
	return decision, nil
}

func (c *RPCClient) Read(ctx context.Context, target, key string) (string, error) {
	return c.ReadFrom(ctx, target, "", key)
}

// ReadFrom issues a linearizable read and tags it with the client's origin
// domain so the Global Leader can record per-domain read load (R_i).
func (c *RPCClient) ReadFrom(ctx context.Context, target, originDomain, key string) (string, error) {
	current := c.leaderOr(target)
	sawRedirect := false
	for ctx.Err() == nil {
		conn, err := c.connection(current)
		if err != nil {
			return "", err
		}
		response, err := cdraftv1.NewClientClient(conn).Read(ctx, &cdraftv1.ClientReadRequest{Key: key, OriginDomain: originDomain})
		if err != nil {
			if ctx.Err() != nil {
				return "", err
			}
			// A cached leader may be stale/down: fall back to the original node.
			c.forgetLeader()
			current = target
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if response.GetRedirectAddress() != "" {
			current = response.GetRedirectAddress()
			c.rememberLeader(current)
			// Follow the first redirect immediately; only back off if we keep
			// bouncing (a Global Leader churn window).
			if sawRedirect {
				time.Sleep(5 * time.Millisecond)
			}
			sawRedirect = true
			continue
		}
		if response.GetError() != "" {
			return "", fmt.Errorf("%s", response.GetError())
		}
		c.rememberLeader(current)
		return response.GetValue(), nil
	}
	return "", ErrNotGlobalLeader
}

func (c *RPCClient) Status(ctx context.Context, target string) (*cdraftv1.NodeStatusResponse, error) {
	conn, err := c.connection(target)
	if err != nil {
		return nil, err
	}
	return cdraftv1.NewClientClient(conn).Status(ctx, &cdraftv1.NodeStatusRequest{})
}

func (c *RPCClient) DiscoverTopology(ctx context.Context, target, originDomain string) (*cdraftv1.TopologyResponse, error) {
	current := c.leaderOr(target)
	sawRedirect := false
	for ctx.Err() == nil {
		conn, err := c.connection(current)
		if err != nil {
			return nil, err
		}
		response, err := cdraftv1.NewClientClient(conn).DiscoverTopology(ctx, &cdraftv1.TopologyRequest{OriginDomain: originDomain})
		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			c.forgetLeader()
			current = target
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if response.GetRedirectAddress() != "" {
			current = response.GetRedirectAddress()
			c.rememberLeader(current)
			if sawRedirect {
				time.Sleep(5 * time.Millisecond)
			}
			sawRedirect = true
			continue
		}
		if response.GetError() != "" {
			return nil, fmt.Errorf("%s", response.GetError())
		}
		if response.GetGlobalLeaderAddress() != "" {
			c.rememberLeader(response.GetGlobalLeaderAddress())
		} else {
			c.rememberLeader(current)
		}
		return response, nil
	}
	return nil, ErrNotGlobalLeader
}

func (c *RPCClient) MeasureFloatingLatencies(ctx context.Context, originDomain string, view *cdraftv1.TopologyResponse) (map[string]float64, error) {
	latencies := make(map[string]float64)
	for _, endpoint := range view.GetDomainLeaders() {
		if endpoint.GetDomainId() == "" || endpoint.GetClientAddress() == "" {
			continue
		}
		start := time.Now()
		conn, err := c.connection(endpoint.GetClientAddress())
		if err != nil {
			return nil, err
		}
		_, err = cdraftv1.NewTelemetryClient(conn).Ping(ctx, &cdraftv1.PingRequest{FromDomain: originDomain})
		if err != nil {
			return nil, err
		}
		latencies[endpoint.GetDomainId()] = float64(time.Since(start).Milliseconds()) / 2
	}
	return latencies, nil
}

func (c *RPCClient) ReportFloatingLatency(ctx context.Context, target, originDomain string, globalTerm uint64, oneWayMillis map[string]float64) error {
	conn, err := c.connection(target)
	if err != nil {
		return err
	}
	report := &cdraftv1.FloatingLatencyReport{OriginDomain: originDomain, GlobalTerm: globalTerm}
	for domain, oneWay := range oneWayMillis {
		if oneWay < 0 {
			continue
		}
		report.Rtts = append(report.Rtts, &cdraftv1.FloatingDomainRtt{ToDomain: domain, OneWayMillis: uint64(oneWay)})
	}
	_, err = cdraftv1.NewTelemetryClient(conn).ReportFloatingLatency(ctx, report)
	return err
}

func (c *RPCClient) Metrics(ctx context.Context, target string) (*cdraftv1.MetricsResponse, error) {
	conn, err := c.connection(target)
	if err != nil {
		return nil, err
	}
	return cdraftv1.NewMetricsClient(conn).Snapshot(ctx, &cdraftv1.NodeStatusRequest{})
}

// Move asks the target node (which must be the current Global Leader) to hand
// the Global Leadership to targetDomain via the safe catch-up handoff. It is the
// administrative trigger used by the external migration controller; the node
// rejects it unless it is itself the GL.
func (c *RPCClient) Move(ctx context.Context, target, targetDomain, reason string) (*cdraftv1.MoveResponse, error) {
	conn, err := c.connection(target)
	if err != nil {
		return nil, err
	}
	return cdraftv1.NewTelemetryClient(conn).Move(ctx, &cdraftv1.MoveRequest{
		TargetDomain: targetDomain, Reason: reason,
	})
}

func (c *RPCClient) writeFollowingRedirects(
	ctx context.Context,
	target string,
	request *cdraftv1.ClientWriteRequest,
) (ClientResult, error) {
	trace := WriteTraceFromContext(ctx)
	current := c.leaderOr(target)
	sawRedirect := false
	// Retry until the deadline. The requestId is stable, so retries (including
	// across a Global Leader migration window) are idempotent: the cluster either
	// returns the already-committed result or finally orders the write once.
	for ctx.Err() == nil {
		conn, err := c.connection(current)
		if err != nil {
			return ClientResult{}, err
		}
		response, err := cdraftv1.NewClientClient(conn).Write(ctx, request)
		if err != nil {
			if ctx.Err() != nil {
				return ClientResult{}, err
			}
			// A cached leader may be stale/down: fall back to the original node.
			c.forgetLeader()
			current = target
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if response.GetRedirectAddress() != "" {
			trace.Mark(WriteTraceEvent{RequestID: request.GetRequestId(), Component: "client", Phase: "client.redirect", Detail: response.GetRedirectAddress()})
			current = response.GetRedirectAddress()
			c.rememberLeader(current)
			if sawRedirect {
				time.Sleep(5 * time.Millisecond)
			}
			sawRedirect = true
			continue
		}
		if response.GetError() != "" {
			return ClientResult{}, fmt.Errorf("%s", response.GetError())
		}
		c.rememberLeader(current)
		return fromPBClientResult(response), nil
	}
	return ClientResult{}, ErrNotGlobalLeader
}

func (c *RPCClient) connection(address string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn := c.conns[address]; conn != nil {
		return conn, nil
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[address] = conn
	return conn, nil
}

// callbackRouter is the single ClientCallback server shared by all writes. It
// dispatches each incoming Fast Return to the channel registered for that
// request id, so one persistent listener serves arbitrarily many writes.
type callbackRouter struct {
	cdraftv1.UnimplementedClientCallbackServer
	mu      sync.Mutex
	waiters map[string]chan Response
	traces  map[string]*WriteTraceRecorder
}

func (r *callbackRouter) register(requestID string, trace *WriteTraceRecorder) <-chan Response {
	ch := make(chan Response, 1)
	r.mu.Lock()
	r.waiters[requestID] = ch
	if trace != nil {
		r.traces[requestID] = trace
	}
	r.mu.Unlock()
	return ch
}

func (r *callbackRouter) unregister(requestID string) {
	r.mu.Lock()
	delete(r.waiters, requestID)
	delete(r.traces, requestID)
	r.mu.Unlock()
}

func (r *callbackRouter) FastReturn(_ context.Context, result *cdraftv1.ClientResult) (*cdraftv1.Empty, error) {
	response := Response{Result: fromPBClientResult(result)}
	r.mu.Lock()
	ch, ok := r.waiters[response.Result.RequestID]
	trace := r.traces[response.Result.RequestID]
	r.mu.Unlock()
	trace.Mark(WriteTraceEvent{
		RequestID: response.Result.RequestID, Component: "client", Phase: "client.fast_callback.received",
		DomainID: response.Result.ResponderDomain,
	})
	if ok {
		// Buffered (cap 1) + non-blocking: a duplicate Fast Return for the same
		// request never blocks the Domain Leader's RPC.
		select {
		case ch <- response:
		default:
		}
	}
	return &cdraftv1.Empty{}, nil
}
