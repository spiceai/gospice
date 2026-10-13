package gospice

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
)

// withCloudAuth makes a test server stand in for Spice Cloud, so Cloud keys use
// the Basic handshake against it.
func withCloudAuth() SpiceClientModifier {
	return func(c *SpiceClient) error {
		c.cloudAuth = true
		return nil
	}
}

func TestWithApiKeyAcceptsAnyNonEmptyKey(t *testing.T) {
	cases := []struct {
		key, appId string
	}{
		{"ro-key", ""},
		{"app|ro-key", "app"},
		{"|secret", ""},
		{"app|", ""},
		{"a|b|c", ""},
	}
	for _, tc := range cases {
		c := NewSpiceClient()
		if err := WithApiKey(tc.key)(c); err != nil {
			t.Fatalf("WithApiKey(%q): %v", tc.key, err)
		}
		if c.apiKey != tc.key || c.appId != tc.appId {
			t.Fatalf("WithApiKey(%q): got appId %q apiKey %q", tc.key, c.appId, c.apiKey)
		}
	}
	if err := WithApiKey("")(NewSpiceClient()); err == nil {
		t.Fatal("expected an error for an empty key")
	}
}

func TestIsCloudAddress(t *testing.T) {
	for addr, want := range map[string]bool{
		defaultCloudConfig.FlightUrl:               true,
		"flight.spiceai.io:443":                    true,
		"grpc+tls://flight.spiceai.io":             true,
		"us-east-1-prod-aws-flight.spiceai.io:443": true,
		"localhost:50051":                          false,
		"grpc://127.0.0.1:50051":                   false,
		"spiceai.io.example.com:443":               false,
		"my-runtime.internal:443":                  false,
	} {
		if got := isCloudAddress(addr); got != want {
			t.Errorf("isCloudAddress(%q) = %v, want %v", addr, got, want)
		}
	}
}

func TestUseBearerAuth(t *testing.T) {
	cases := []struct {
		addr, key string
		want      bool
	}{
		{"localhost:50051", "ro-key", true},
		{"localhost:50051", "app|ro-key", true},
		{"flight.spiceai.io:443", "ro-key", true},
		{"flight.spiceai.io:443", "app|secret", false},
		{"localhost:50051", "", false},
	}
	for _, tc := range cases {
		c := NewSpiceClientWithAddress(tc.addr)
		if tc.key != "" {
			if err := WithApiKey(tc.key)(c); err != nil {
				t.Fatal(err)
			}
		}
		if got := c.useBearerAuth(); got != tc.want {
			t.Errorf("useBearerAuth(%q, %q) = %v, want %v", tc.addr, tc.key, got, tc.want)
		}
	}
}

func TestAdbcOptionsAuth(t *testing.T) {
	local := NewSpiceClientWithAddress("localhost:50051")
	_ = WithApiKey("ro-key")(local)
	opts, err := local.adbcOptions()
	if err != nil {
		t.Fatal(err)
	}
	if got := opts[flightsql.OptionAuthorizationHeader]; got != "Bearer ro-key" {
		t.Fatalf("local plain key: authorization header %q", got)
	}
	if _, ok := opts[adbc.OptionKeyUsername]; ok {
		t.Fatal("local plain key: username must not be set")
	}

	cloud := NewSpiceClientWithAddress("flight.spiceai.io:443")
	_ = WithApiKey("app|secret")(cloud)
	opts, err = cloud.adbcOptions()
	if err != nil {
		t.Fatal(err)
	}
	if opts[adbc.OptionKeyUsername] != "app" || opts[adbc.OptionKeyPassword] != "app|secret" {
		t.Fatalf("cloud key: unexpected basic credentials %v", opts)
	}
	if _, ok := opts[flightsql.OptionAuthorizationHeader]; ok {
		t.Fatal("cloud key: authorization header must not be set")
	}
}

// A plain key goes to the runtime as "Authorization: Bearer <key>" on every
// call, with no Basic handshake.
func TestPlainKeyIsSentAsBearerWithoutHandshake(t *testing.T) {
	for _, key := range []string{"ro-key", "app|ro-key"} {
		srv := newSessionTestServer()
		srv.mu.Lock()
		srv.sessions[key] = true
		srv.mu.Unlock()
		spice := NewSpiceClient()
		if err := spice.Init(WithFlightAddress(startSessionTestServer(t, srv)), WithApiKey(key)); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			rdr, err := spice.Sql(context.Background(), "SELECT 1")
			if err != nil {
				t.Fatalf("key %q query %d: %v", key, i, err)
			}
			rdr.Release()
		}
		attempts, _ := srv.counts()
		if attempts != 0 {
			t.Fatalf("key %q: expected no handshake, got %d", key, attempts)
		}
		srv.mu.Lock()
		for _, r := range srv.requests {
			if r.authorization != "Bearer "+key {
				t.Fatalf("key %q: %s sent authorization %q", key, r.rpc, r.authorization)
			}
		}
		srv.mu.Unlock()
		_ = spice.Close()
	}
}
