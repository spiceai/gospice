package gospice

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

// emptyEndpointServer is a minimal in-process Flight server whose GetFlightInfo
// answers with a FlightInfo carrying no endpoints, so the client's handling of
// that response can be exercised without a real spiced.
type emptyEndpointServer struct {
	flight.BaseFlightServer
}

func (s *emptyEndpointServer) GetFlightInfo(_ context.Context, _ *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{Endpoint: nil}, nil
}

func startEmptyEndpointServer(t *testing.T) string {
	t.Helper()

	fs := flight.NewServerWithMiddleware(nil)
	if err := fs.Init("127.0.0.1:0"); err != nil {
		t.Fatalf("error starting test flight server: %v", err)
	}
	fs.RegisterFlightService(&emptyEndpointServer{})

	go func() {
		_ = fs.Serve()
	}()
	t.Cleanup(fs.Shutdown)

	return "grpc://" + fs.Addr().String()
}

// TestSqlNoFlightEndpoint checks that a FlightInfo with no endpoints is reported
// as an error rather than panicking on an out-of-range index.
func TestSqlNoFlightEndpoint(t *testing.T) {
	spice := NewSpiceClient()
	if err := spice.Init(WithFlightAddress(startEmptyEndpointServer(t))); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}
	t.Cleanup(func() { _ = spice.Close() })

	reader, err := spice.Sql(context.Background(), "SELECT 1")
	if err == nil {
		if reader != nil {
			reader.Release()
		}
		t.Fatal("expected an error when the server returns no Flight endpoint, got nil")
	}

	if !strings.Contains(err.Error(), "no Flight endpoint") {
		t.Fatalf("expected error to mention the missing Flight endpoint, got: %v", err)
	}
}
