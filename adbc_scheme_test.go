package gospice

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

// TestADBCOptionsSchemeMatchesFlight checks that SqlWithParams (ADBC) and Sql
// (native Flight) choose the same transport for the same Flight address. A bare
// host:port is TLS on both; only an explicit grpc:// is plaintext.
func TestADBCOptionsSchemeMatchesFlight(t *testing.T) {
	cases := []struct {
		address string
		wantURI string
		wantTLS bool
	}{
		// Bare host:port is TLS regardless of host or port.
		{"127.0.0.1:50051", "grpc+tls://127.0.0.1:50051", true},
		{"localhost:50051", "grpc+tls://localhost:50051", true},
		{"[::1]:50051", "grpc+tls://[::1]:50051", true},
		{"runtime.internal.example:50051", "grpc+tls://runtime.internal.example:50051", true},
		{"flight.spiceai.io:443", "grpc+tls://flight.spiceai.io:443", true},
		// Used to be plaintext: the old substring heuristic was case-sensitive.
		{"HOST.SPICEAI.IO:50698", "grpc+tls://HOST.SPICEAI.IO:50698", true},
		// Hosts and ports that only look like Cloud or :443 to a substring
		// match. They are TLS because every bare address is, not because they
		// matched anything.
		{"127.0.0.1:4430", "grpc+tls://127.0.0.1:4430", true},
		{"spiceai.io.example.com:50051", "grpc+tls://spiceai.io.example.com:50051", true},
		{"evil-spiceai.io.attacker.example:50051", "grpc+tls://evil-spiceai.io.attacker.example:50051", true},
		// Explicit schemes are respected.
		{"grpc+tls://runtime.internal.example:50051", "grpc+tls://runtime.internal.example:50051", true},
		{"grpc://127.0.0.1:50051", "grpc://127.0.0.1:50051", false},
		{"grpc://runtime.internal.example:50051", "grpc://runtime.internal.example:50051", false},
	}
	for _, tc := range cases {
		t.Run(tc.address, func(t *testing.T) {
			spice := NewSpiceClient()
			spice.flightAddress = tc.address

			options, err := spice.adbcOptions()
			if err != nil {
				t.Fatalf("adbcOptions: %v", err)
			}
			if got := options[adbc.OptionKeyURI]; got != tc.wantURI {
				t.Errorf("SqlWithParams URI = %q, want %q", got, tc.wantURI)
			}

			// createClient (Sql) dials with exactly this decision.
			target, useTLS := flightTransport(tc.address)
			if useTLS != tc.wantTLS {
				t.Errorf("Sql uses TLS = %v, want %v", useTLS, tc.wantTLS)
			}
			scheme := "grpc://"
			if useTLS {
				scheme = "grpc+tls://"
			}
			adbcTarget, ok := strings.CutPrefix(options[adbc.OptionKeyURI], scheme)
			if !ok || target != adbcTarget {
				t.Errorf("Sql dials %q, SqlWithParams dials %q", target, adbcTarget)
			}
		})
	}
}

// TestADBCOptionsBareAddressNeverSendsKeyInCleartext points a client with an
// API key at a plain TCP listener on a bare host:port and records every byte
// SqlWithParams writes to it. The address is on a random non-443 port with no
// Cloud host in it, the case the old heuristic sent in plaintext. The
// connection has to open with a TLS handshake and the key must never appear on
// the wire.
func TestADBCOptionsBareAddressNeverSendsKeyInCleartext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var (
		mu         sync.Mutex
		wire       [][]byte
		connWG     sync.WaitGroup
		acceptDone = make(chan struct{})
	)
	go func() {
		defer close(acceptDone)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			connWG.Add(1)
			go func(conn net.Conn) {
				defer connWG.Done()
				defer func() { _ = conn.Close() }()
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				var buf bytes.Buffer
				chunk := make([]byte, 4096)
				for buf.Len() < 64*1024 {
					n, err := conn.Read(chunk)
					buf.Write(chunk[:n])
					if err != nil {
						break
					}
				}
				mu.Lock()
				wire = append(wire, buf.Bytes())
				mu.Unlock()
			}(conn)
		}
	}()

	const key = "plaintext-canary-api-key"
	spice := NewSpiceClient()
	t.Cleanup(func() { _ = spice.Close() })
	spice.SetMaxRetries(0)
	// Init may or may not fail against a listener that never answers; only
	// what reaches the wire matters here.
	_ = spice.Init(WithFlightAddress(ln.Addr().String()), WithApiKey(key))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if rdr, err := spice.SqlWithParams(ctx, "SELECT ?", 1); err == nil {
		rdr.Release()
		t.Fatal("SqlWithParams against a non-Flight listener succeeded")
	}
	_ = ln.Close()
	<-acceptDone // no more connWG.Add after this
	connWG.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(wire) == 0 {
		t.Fatal("SqlWithParams never connected to the listener")
	}
	for i, b := range wire {
		if bytes.Contains(b, []byte(key)) {
			t.Errorf("connection %d sent the API key in cleartext", i)
		}
		if len(b) > 0 && b[0] != 0x16 {
			t.Errorf("connection %d opened with byte %#x, want a TLS handshake record (0x16)", i, b[0])
		}
	}
}

// TestIsCloudAddressParsesHost checks that Cloud detection, which picks the
// authentication scheme, reads the host out of the address rather than
// matching substrings.
func TestIsCloudAddressParsesHost(t *testing.T) {
	for address, want := range map[string]bool{
		"flight.spiceai.io:443":                         true,
		"HOST.SPICEAI.IO:50698":                         true,
		"grpc+tls://flight.spiceai.io:443":              true,
		"127.0.0.1:4430":                                false,
		"127.0.0.1:443":                                 false,
		"spiceai.io.example.com:443":                    false,
		"evil-spiceai.io.attacker.example:443":          false,
		"grpc+tls://evil-spiceai.io.attacker.example:1": false,
	} {
		if got := isCloudAddress(address); got != want {
			t.Errorf("isCloudAddress(%q) = %v, want %v", address, got, want)
		}
	}
}
