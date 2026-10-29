package gospice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The client's HTTP endpoint has to agree with its Flight endpoint. A client
// pointed at a local runtime that sends its HTTP calls to Spice Cloud reports
// on a runtime the caller never asked about — IsSpiceHealthy answers "healthy"
// from Cloud while the local runtime the queries go to is down.
func TestHttpDefaultFollowsFlightDefault(t *testing.T) {
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer local.Close()

	cloudHits := 0
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudHits++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer cloud.Close()

	savedLocal, savedCloud := defaultLocalConfig, defaultCloudConfig
	defer func() { defaultLocalConfig, defaultCloudConfig = savedLocal, savedCloud }()
	defaultLocalConfig.HttpUrl = local.URL
	defaultCloudConfig.HttpUrl = cloud.URL

	spice := NewSpiceClient()
	defer func() { _ = spice.Close() }()

	if err := spice.Init(); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	if !spice.IsSpiceHealthy(context.Background()) {
		t.Fatalf("expected the local runtime stand-in to answer the health check")
	}
	if cloudHits != 0 {
		t.Fatalf("a client on the local Flight endpoint sent %d HTTP request(s) to Spice Cloud; want 0", cloudHits)
	}
	if spice.baseHttpUrl != local.URL {
		t.Fatalf("baseHttpUrl = %q, want the local runtime URL %q", spice.baseHttpUrl, local.URL)
	}
}

// WithSpiceCloudAddress moves the Flight endpoint to Spice Cloud, so the HTTP
// endpoint has to follow it there.
func TestHttpDefaultFollowsSpiceCloudAddress(t *testing.T) {
	savedCloud := defaultCloudConfig
	defer func() { defaultCloudConfig = savedCloud }()
	defaultCloudConfig.HttpUrl = "https://data.example.invalid"

	spice := NewSpiceClient()
	defer func() { _ = spice.Close() }()

	if err := spice.Init(WithSpiceCloudAddress()); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	if spice.baseHttpUrl != defaultCloudConfig.HttpUrl {
		t.Fatalf("baseHttpUrl = %q, want the Spice Cloud URL %q", spice.baseHttpUrl, defaultCloudConfig.HttpUrl)
	}
}

// An explicitly named HTTP endpoint is never derived over, whichever order the
// options arrive in.
func TestWithHttpAddressWinsOverTheDefault(t *testing.T) {
	const custom = "http://runtime.example.invalid:8090"

	for _, tt := range []struct {
		name string
		opts []SpiceClientModifier
	}{
		{"http address last", []SpiceClientModifier{WithSpiceCloudAddress(), WithHttpAddress(custom)}},
		{"http address first", []SpiceClientModifier{WithHttpAddress(custom), WithSpiceCloudAddress()}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spice := NewSpiceClient()
			defer func() { _ = spice.Close() }()

			if err := spice.Init(tt.opts...); err != nil {
				t.Fatalf("error initializing SpiceClient: %v", err)
			}
			if spice.baseHttpUrl != custom {
				t.Fatalf("baseHttpUrl = %q, want %q", spice.baseHttpUrl, custom)
			}
		})
	}
}

// A Flight endpoint that is neither of the two defaults says nothing about where
// that runtime serves HTTP, so the pairing must not claim it does: deriving the
// local HTTP URL for every unrecognised address would point a self-hosted client
// — and the API key it was configured with — at whatever listens on this machine.
// Such a client keeps the HTTP endpoint it already had.
func TestCustomFlightAddressDoesNotDeriveTheLocalHttpUrl(t *testing.T) {
	savedLocal, savedCloud := defaultLocalConfig, defaultCloudConfig
	defer func() { defaultLocalConfig, defaultCloudConfig = savedLocal, savedCloud }()
	defaultLocalConfig.HttpUrl = "http://localhost.invalid:8090"
	defaultCloudConfig.HttpUrl = "https://data.example.invalid"

	const customFlight = "grpc+tls://runtime.example.invalid:50051"

	for _, tt := range []struct {
		name  string
		build func() *SpiceClient
		opts  []SpiceClientModifier
	}{
		{
			name:  "NewSpiceClientWithAddress",
			build: func() *SpiceClient { return NewSpiceClientWithAddress(customFlight) },
		},
		{
			name:  "WithFlightAddress",
			build: NewSpiceClient,
			opts:  []SpiceClientModifier{WithFlightAddress(customFlight)},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spice := tt.build()
			defer func() { _ = spice.Close() }()

			if err := spice.Init(tt.opts...); err != nil {
				t.Fatalf("error initializing SpiceClient: %v", err)
			}
			if spice.baseHttpUrl == defaultLocalConfig.HttpUrl {
				t.Fatalf("a custom Flight address derived the local HTTP URL %q", spice.baseHttpUrl)
			}
		})
	}
}

// A custom Flight address pairs with the runtime's default HTTP port on the
// same host, never with Spice Cloud: the client's HTTP calls, and the API key
// they carry, only reach a host the caller configured.
func TestCustomFlightAddressDerivesHttpUrlOnTheSameHost(t *testing.T) {
	savedLocal, savedCloud := defaultLocalConfig, defaultCloudConfig
	defer func() { defaultLocalConfig, defaultCloudConfig = savedLocal, savedCloud }()
	defaultLocalConfig.FlightUrl = "grpc://127.0.0.1:50051"
	defaultLocalConfig.HttpUrl = "http://127.0.0.1:8090"
	defaultCloudConfig.FlightUrl = "flight.spiceai.io:443"
	defaultCloudConfig.HttpUrl = "https://data.example.invalid"
	defaultCloudConfig.httpUrlFromEnv = false

	for _, tt := range []struct {
		flight string
		want   string
	}{
		{"grpc://127.0.0.1:15151", "http://127.0.0.1:8090"},
		{"grpc://runtime.example.invalid:50051", "http://runtime.example.invalid:8090"},
		{"grpc+tcp://runtime.example.invalid:50051", "http://runtime.example.invalid:8090"},
		{"grpc+tls://runtime.example.invalid:50051", "https://runtime.example.invalid:8090"},
		{"runtime.example.invalid:443", "https://runtime.example.invalid:8090"},
		{"grpc://[::1]:15151", "http://[::1]:8090"},
		{"grpc://runtime.example.invalid", "http://runtime.example.invalid:8090"},
		// Spice Cloud Flight hosts other than the default still use Spice Cloud HTTP.
		{"grpc+tls://us-west-2.flight.spiceai.io:443", defaultCloudConfig.HttpUrl},
		// Known pairings are unchanged.
		{"grpc://127.0.0.1:50051", "http://127.0.0.1:8090"},
		{"flight.spiceai.io:443", defaultCloudConfig.HttpUrl},
		// gRPC resolver targets: the application endpoint is the path, and a
		// dns:// authority names the resolver, never the host to send HTTP to.
		{"dns:///runtime.example.invalid:443", "https://runtime.example.invalid:8090"},
		{"dns://resolver.example.invalid:53/runtime.example.invalid:443", "https://runtime.example.invalid:8090"},
		{"dns:///flight.spiceai.io:443", defaultCloudConfig.HttpUrl},
		{"passthrough:///runtime.example.invalid:50051", "https://runtime.example.invalid:8090"},
		// No readable host: no HTTP endpoint at all rather than a guessed one.
		{"grpc://", ""},
		{"dns://resolver.example.invalid:53", ""},
		{"unix:///tmp/spice.sock", ""},
		{"unix:/tmp/spice.sock", ""},
	} {
		t.Run(tt.flight, func(t *testing.T) {
			spice := NewSpiceClient()
			defer func() { _ = spice.Close() }()
			if err := spice.Init(WithFlightAddress(tt.flight)); err != nil {
				t.Fatalf("error initializing SpiceClient: %v", err)
			}
			if spice.baseHttpUrl != tt.want {
				t.Fatalf("Init(WithFlightAddress(%q)): baseHttpUrl = %q, want %q", tt.flight, spice.baseHttpUrl, tt.want)
			}
			if got := NewSpiceClientWithAddress(tt.flight).baseHttpUrl; got != tt.want {
				t.Fatalf("NewSpiceClientWithAddress(%q): baseHttpUrl = %q, want %q", tt.flight, got, tt.want)
			}
		})
	}
}

// With only a custom Flight address and an API key configured, RefreshDataset
// and IsSpiceReady must not contact Spice Cloud or send it the key.
func TestCustomFlightAddressNeverSendsApiKeyToSpiceCloud(t *testing.T) {
	cloudHits := 0
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudHits++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer cloud.Close()

	savedCloud := defaultCloudConfig
	defer func() { defaultCloudConfig = savedCloud }()
	defaultCloudConfig.HttpUrl = cloud.URL
	defaultCloudConfig.httpUrlFromEnv = false

	spice := NewSpiceClient()
	defer func() { _ = spice.Close() }()
	if err := spice.Init(
		WithFlightAddress("grpc://runtime.example.invalid:15151"),
		WithApiKey("local-runtime-key"),
	); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}
	spice.maxRetries = 0

	ctx := context.Background()
	err := spice.RefreshDataset(ctx, "some_dataset", nil)
	if err == nil {
		t.Fatalf("RefreshDataset against an unreachable custom host succeeded; want a connection error")
	}
	if !strings.Contains(err.Error(), "runtime.example.invalid") {
		t.Fatalf("RefreshDataset error %q does not name the configured host", err)
	}
	if spice.IsSpiceReady(ctx) {
		t.Fatalf("IsSpiceReady reported ready for an unreachable custom host")
	}
	if cloudHits != 0 {
		t.Fatalf("a client on a custom Flight address sent %d HTTP request(s) to Spice Cloud; want 0", cloudHits)
	}
}

// An HTTP endpoint configured explicitly through SPICE_HTTP_URL keeps applying
// to a client on a custom Flight address, as it did before same-host pairing.
func TestSpiceHttpUrlEnvOverrideIsKeptForCustomFlightAddress(t *testing.T) {
	savedCloud := defaultCloudConfig
	defer func() { defaultCloudConfig = savedCloud }()

	t.Setenv("SPICE_HTTP_URL", "https://runtime.example.invalid:8443")
	defaultCloudConfig = LoadConfig()

	spice := NewSpiceClient()
	defer func() { _ = spice.Close() }()
	if err := spice.Init(WithFlightAddress("grpc://runtime.example.invalid:50051")); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}
	if want := "https://runtime.example.invalid:8443"; spice.baseHttpUrl != want {
		t.Fatalf("baseHttpUrl = %q, want the SPICE_HTTP_URL endpoint %q", spice.baseHttpUrl, want)
	}
}
