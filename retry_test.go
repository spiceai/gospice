package gospice

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
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

func TestFlightServiceConfigOmitsRetryPolicyWhenRetriesDisabled(t *testing.T) {
	c := NewSpiceClient()
	c.SetMaxRetries(0)
	if cfg := c.flightServiceConfig(); cfg != "" {
		t.Fatalf("flightServiceConfig() = %q, want empty when retries are disabled", cfg)
	}

	c.SetMaxRetries(3)
	cfg := c.flightServiceConfig()
	if !strings.Contains(cfg, `"MaxAttempts": 4`) {
		t.Fatalf("flightServiceConfig() = %s, want MaxAttempts 4 (maxRetries+1)", cfg)
	}
	if !strings.Contains(cfg, `"retryPolicy"`) {
		t.Fatalf("flightServiceConfig() = %s, want a retryPolicy when retries are enabled", cfg)
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
