package gospice

import (
	"context"
	"net/http"
	"net/http/httptest"
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
