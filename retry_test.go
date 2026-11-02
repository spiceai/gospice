package gospice

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type unavailableFlight struct {
	flight.BaseFlightServer
	calls atomic.Int32
}

func (s *unavailableFlight) GetFlightInfo(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s.calls.Add(1)
	return nil, status.Error(codes.Unavailable, "unavailable")
}

func TestWithRetriesUnwrapsPermanentWhenDisabled(t *testing.T) {
	c := NewSpiceClient()
	c.SetMaxRetries(0)
	inner := errors.New("not retryable")
	err := c.withRetries(context.Background(), func() error {
		return backoff.Permanent(inner)
	})
	if !errors.Is(err, inner) {
		t.Fatalf("withRetries() = %v, want unwrapped %v", err, inner)
	}
	var permanent *backoff.PermanentError
	if errors.As(err, &permanent) {
		t.Fatalf("withRetries leaked *backoff.PermanentError: %v", err)
	}
}

func TestSetMaxRetriesZeroInitSucceeds(t *testing.T) {
	c := NewSpiceClient()
	defer func() { _ = c.Close() }()
	c.SetMaxRetries(0)
	if err := c.Init(WithFlightAddress("grpc://127.0.0.1:1")); err != nil {
		t.Fatalf("SetMaxRetries(0) + Init: %v", err)
	}
}

func TestSetMaxRetriesZeroAttemptsOnce(t *testing.T) {
	svc := &unavailableFlight{}
	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(svc)
	if err := srv.Init("127.0.0.1:0"); err != nil {
		t.Fatalf("start flight server: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(srv.Shutdown)

	c := NewSpiceClient()
	defer func() { _ = c.Close() }()
	c.SetMaxRetries(0)
	addr := "grpc://" + srv.Addr().String()
	if err := c.Init(WithFlightAddress(addr)); err != nil {
		t.Fatalf("SetMaxRetries(0) + Init(%s): %v", addr, err)
	}

	before := svc.calls.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.Sql(ctx, "SELECT 1")
	if err == nil {
		t.Fatal("Sql against an unavailable Flight server succeeded, want an error")
	}
	got := svc.calls.Load() - before
	if got != 1 {
		t.Fatalf("GetFlightInfo calls after Sql = %d, want 1 (retries disabled); total=%d init=%d err=%v",
			got, svc.calls.Load(), before, err)
	}
}

// deadFlightAddress returns a grpc:// address on 127.0.0.1 that nothing
// listens on: a port that was just bound and released.
func deadFlightAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return "grpc://" + addr
}

// unreachableBudget bounds how long a call against an unreachable runtime may
// take with the default 3 retries: their backoff adds up to well under 3s.
const unreachableBudget = 5 * time.Second

// An unreachable runtime fails fast with Unavailable after the configured
// retries, whatever the ctx deadline and whether SetMaxRetries was called
// before Init, after it, or not at all. It used to block until the deadline:
// the connection waited for the runtime to become ready.
func TestUnreachableSqlFailsFastWithUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		before  *uint
		after   *uint
		minTime time.Duration // retries back off, so more than one attempt takes time
	}{
		{name: "default retries", minTime: 100 * time.Millisecond},
		{name: "SetMaxRetries(0) before Init", before: ptr[uint](0)},
		{name: "SetMaxRetries(0) after Init", after: ptr[uint](0)},
		{name: "SetMaxRetries(3) before Init", before: ptr[uint](3), minTime: 100 * time.Millisecond},
		{name: "SetMaxRetries(3) after Init", after: ptr[uint](3), minTime: 100 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewSpiceClient()
			defer func() { _ = c.Close() }()
			if tc.before != nil {
				c.SetMaxRetries(*tc.before)
			}
			if err := c.Init(WithFlightAddress(deadFlightAddress(t)), WithHttpAddress("http://127.0.0.1:1")); err != nil {
				t.Fatalf("Init: %v", err)
			}
			if tc.after != nil {
				c.SetMaxRetries(*tc.after)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			start := time.Now()
			_, err := c.Sql(ctx, "SELECT 1")
			elapsed := time.Since(start)
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("Sql against an unreachable runtime = %v (code %v), want Unavailable", err, status.Code(err))
			}
			if elapsed > unreachableBudget {
				t.Fatalf("Sql took %v against an unreachable runtime, want under %v", elapsed, unreachableBudget)
			}
			if elapsed < tc.minTime {
				t.Fatalf("Sql returned after %v, want at least %v of retry backoff", elapsed, tc.minTime)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// With context.Background(), as in the README examples, a call against an
// unreachable runtime returns instead of hanging forever.
func TestUnreachableSqlWithBackgroundContextReturns(t *testing.T) {
	c := NewSpiceClient()
	defer func() { _ = c.Close() }()
	if err := c.Init(WithFlightAddress(deadFlightAddress(t)), WithHttpAddress("http://127.0.0.1:1")); err != nil {
		t.Fatalf("Init: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.Sql(context.Background(), "SELECT 1")
		done <- err
	}()
	select {
	case err := <-done:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("Sql = %v, want Unavailable", err)
		}
	case <-time.After(2 * unreachableBudget):
		t.Fatalf("Sql(context.Background()) against an unreachable runtime has not returned after %v", 2*unreachableBudget)
	}
}

// WithWaitForReady(true) keeps the old behavior for callers that want it: the
// call waits for the runtime instead of failing, until ctx ends.
func TestWaitForReadyWaitsUntilContextEnds(t *testing.T) {
	c := NewSpiceClient()
	defer func() { _ = c.Close() }()
	if err := c.Init(WithFlightAddress(deadFlightAddress(t)), WithHttpAddress("http://127.0.0.1:1"), WithWaitForReady(true)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	const wait = 1500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	start := time.Now()
	_, err := c.Sql(ctx, "SELECT 1")
	elapsed := time.Since(start)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Sql with WithWaitForReady = %v (code %v), want DeadlineExceeded", err, status.Code(err))
	}
	if elapsed < wait-100*time.Millisecond {
		t.Fatalf("Sql with WithWaitForReady returned after %v, want it to wait about %v", elapsed, wait)
	}
}

// WithWaitForReady(true) lets a call issued before the runtime is up succeed
// once it starts.
func TestWaitForReadySucceedsOnceRuntimeStarts(t *testing.T) {
	addr := deadFlightAddress(t)
	c := NewSpiceClient()
	defer func() { _ = c.Close() }()
	c.SetMaxRetries(0)
	if err := c.Init(WithFlightAddress(addr), WithHttpAddress("http://127.0.0.1:1"), WithWaitForReady(true)); err != nil {
		t.Fatalf("Init: %v", err)
	}

	svc := &unavailableFlight{}
	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(svc)
	go func() {
		time.Sleep(300 * time.Millisecond)
		if err := srv.Init(strings.TrimPrefix(addr, "grpc://")); err != nil {
			t.Errorf("start flight server: %v", err)
			return
		}
		_ = srv.Serve()
	}()
	t.Cleanup(srv.Shutdown)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := c.Sql(ctx, "SELECT 1")
	// The stand-in server answers Unavailable: reaching it is the point.
	if status.Code(err) != codes.Unavailable || svc.calls.Load() != 1 {
		t.Fatalf("Sql with WithWaitForReady = %v after %d server calls, want the server's Unavailable after 1 call", err, svc.calls.Load())
	}
}

// SetMaxRetries takes effect on the next call even after Init: the server
// sees exactly 1 + maxRetries attempts, with no extra gRPC-level retries
// stacked on top.
func TestSetMaxRetriesAfterInitTakesEffect(t *testing.T) {
	svc := &unavailableFlight{}
	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(svc)
	if err := srv.Init("127.0.0.1:0"); err != nil {
		t.Fatalf("start flight server: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(srv.Shutdown)

	c := NewSpiceClient()
	defer func() { _ = c.Close() }()
	if err := c.Init(WithFlightAddress("grpc://" + srv.Addr().String())); err != nil {
		t.Fatalf("Init: %v", err)
	}

	for _, retries := range []uint{0, 2, 1} {
		c.SetMaxRetries(retries)
		before := svc.calls.Load()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := c.Sql(ctx, "SELECT 1")
		cancel()
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("SetMaxRetries(%d): Sql = %v, want Unavailable", retries, err)
		}
		if got, want := svc.calls.Load()-before, int32(retries)+1; got != want {
			t.Fatalf("SetMaxRetries(%d) after Init: server saw %d attempts, want %d", retries, got, want)
		}
	}
}

// Retrying stops when ctx ends and reports the last attempt's error rather
// than a bare context error.
func TestWithRetriesStopsWhenContextEnds(t *testing.T) {
	c := NewSpiceClient()
	c.SetMaxRetries(100)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	attemptErr := status.Error(codes.Unavailable, "down")
	start := time.Now()
	err := c.withRetries(ctx, func() error { return attemptErr })
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("withRetries ran %v past a 300ms ctx", elapsed)
	}
	if !errors.Is(err, attemptErr) {
		t.Fatalf("withRetries = %v, want the last attempt's error %v", err, attemptErr)
	}
}

// SetMaxRetries may be called while queries run.
func TestSetMaxRetriesConcurrentWithCalls(t *testing.T) {
	c := NewSpiceClient()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.withRetries(context.Background(), func() error { return backoff.Permanent(errors.New("x")) })
		}()
	}
	c.SetMaxRetries(1)
	wg.Wait()
}
