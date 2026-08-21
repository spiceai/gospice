package gospice

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// isClosed reports whether the connection has actually been closed, reading the
// flag under the same lock the lease bookkeeping uses.
func (a *ADBCClient) isClosed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}

// newLeaseTestClient returns an ADBCClient with no underlying db or connection.
// closeNow is a no-op on those, so the lease state machine can be exercised
// without dialing a runtime.
func newLeaseTestClient() *ADBCClient {
	return &ADBCClient{}
}

func TestADBCRetireClosesOnlyWhenUnused(t *testing.T) {
	t.Run("retire with no leases closes immediately", func(t *testing.T) {
		c := newLeaseTestClient()
		if err := c.retire(); err != nil {
			t.Fatalf("retire: %v", err)
		}
		if !c.isClosed() {
			t.Error("an unused connection should close as soon as it is retired")
		}
	})

	t.Run("retire while leased defers the close", func(t *testing.T) {
		c := newLeaseTestClient()
		c.acquire()

		if err := c.retire(); err != nil {
			t.Fatalf("retire: %v", err)
		}
		if c.isClosed() {
			t.Fatal("retiring a connection still in use must not close it - this is the use-after-close a concurrent query would hit")
		}

		c.release()
		if !c.isClosed() {
			t.Error("the last release of a retired connection should close it")
		}
	})

	t.Run("close waits for the last of several leases", func(t *testing.T) {
		c := newLeaseTestClient()
		c.acquire()
		c.acquire()
		c.acquire()

		if err := c.retire(); err != nil {
			t.Fatalf("retire: %v", err)
		}

		c.release()
		c.release()
		if c.isClosed() {
			t.Fatal("closed while a lease was still held")
		}

		c.release()
		if !c.isClosed() {
			t.Error("should close once every lease is released")
		}
	})

	t.Run("release without retire never closes", func(t *testing.T) {
		c := newLeaseTestClient()
		c.acquire()
		c.release()
		if c.isClosed() {
			t.Error("a live connection must survive a query finishing")
		}
	})
}

func TestADBCLeaseIsRaceFree(t *testing.T) {
	// Run under -race: concurrent acquires, releases and a retire must not
	// close the connection early or close it twice.
	c := newLeaseTestClient()

	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)

	c.acquire() // held for the duration so the retire below cannot close early
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			c.acquire()
			c.release()
		}()
	}

	if err := c.retire(); err != nil {
		t.Fatalf("retire: %v", err)
	}
	wg.Wait()

	if c.isClosed() {
		t.Fatal("closed while the outer lease was still held")
	}
	c.release()
	if !c.isClosed() {
		t.Error("should close after the final release")
	}
}

// fakeRecordReader is a minimal array.RecordReader for exercising the lease
// wrapper's refcount. refs starts at 1: a reader handed to newLeasedRecordReader
// already carries the reference its creator holds.
type fakeRecordReader struct {
	refs     int
	released int
}

func newFakeRecordReader() *fakeRecordReader {
	return &fakeRecordReader{refs: 1}
}

func (f *fakeRecordReader) Retain()                        { f.refs++ }
func (f *fakeRecordReader) Release()                       { f.refs--; f.released++ }
func (f *fakeRecordReader) Schema() *arrow.Schema          { return nil }
func (f *fakeRecordReader) Next() bool                     { return false }
func (f *fakeRecordReader) RecordBatch() arrow.RecordBatch { return nil }
func (f *fakeRecordReader) Record() arrow.RecordBatch      { return nil }
func (f *fakeRecordReader) Err() error                     { return nil }

func TestLeasedRecordReaderHoldsConnectionOpen(t *testing.T) {
	// A reader returned by SqlWithParams goes on streaming from the connection
	// after the call returns, so retiring the connection must not close it
	// until the reader is released.
	conn := newLeaseTestClient()
	conn.acquire()

	inner := newFakeRecordReader()
	rdr := newLeasedRecordReader(inner, conn)

	if err := conn.retire(); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if conn.isClosed() {
		t.Fatal("connection closed while a returned reader was still streaming from it")
	}

	rdr.Release()
	if inner.released != 1 {
		t.Errorf("underlying reader released %d times, want 1", inner.released)
	}
	if !conn.isClosed() {
		t.Error("releasing the reader should drop the last lease and close the connection")
	}
}

func TestLeasedRecordReaderBalancesRetain(t *testing.T) {
	conn := newLeaseTestClient()
	conn.acquire()

	inner := newFakeRecordReader()
	rdr := newLeasedRecordReader(inner, conn)

	rdr.Retain()
	if err := conn.retire(); err != nil {
		t.Fatalf("retire: %v", err)
	}

	rdr.Release()
	if conn.isClosed() {
		t.Fatal("closed while a retained reference was outstanding")
	}

	rdr.Release()
	if !conn.isClosed() {
		t.Error("should close once the retained reference is also released")
	}
	if inner.refs != 0 {
		t.Errorf("underlying refcount = %d, want 0", inner.refs)
	}
}

func TestIsADBCAuthStatus(t *testing.T) {
	// Unauthorized means the credential is valid but lacks permission, which a
	// fresh handshake cannot fix - reconnecting on it would retire the
	// connection out from under concurrent queries for nothing.
	tests := []struct {
		name string
		code adbc.Status
		want bool
	}{
		{name: "unauthenticated reconnects", code: adbc.StatusUnauthenticated, want: true},
		{name: "unauthorized does not", code: adbc.StatusUnauthorized, want: false},
		{name: "unrelated status does not", code: adbc.StatusIO, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isADBCAuthStatus(tt.code); got != tt.want {
				t.Errorf("isADBCAuthStatus(%v) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}

func TestADBCOptionsCarryTLSMaterial(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "client.pem")
	keyFile := filepath.Join(dir, "client.key")

	for path, content := range map[string]string{
		caFile:   "-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n",
		certFile: "-----BEGIN CERTIFICATE-----\nclient\n-----END CERTIFICATE-----\n",
		keyFile:  "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----\n",
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
	}

	t.Run("mTLS and root CA reach the driver", func(t *testing.T) {
		spice := NewSpiceClient()
		spice.flightAddress = "127.0.0.1:50051"
		spice.tlsRootCertFile = caFile
		spice.tlsClientCertFile = certFile
		spice.tlsClientKeyFile = keyFile

		options, err := spice.adbcOptions()
		if err != nil {
			t.Fatalf("adbcOptions: %v", err)
		}

		if got := options[flightsql.OptionSSLRootCerts]; got == "" {
			t.Error("root CA missing: SqlWithParams would fail verification against a private-CA runtime that Init and the HTTP methods reach fine")
		}
		if got := options[flightsql.OptionMTLSCertChain]; got == "" {
			t.Error("client certificate chain missing")
		}
		if got := options[flightsql.OptionMTLSPrivateKey]; got == "" {
			t.Error("client private key missing")
		}

		// TLS material is meaningless over a plaintext connection.
		if got := options[adbc.OptionKeyURI]; got != "grpc+tls://127.0.0.1:50051" {
			t.Errorf("URI = %q, want the TLS scheme forced when certificates are configured", got)
		}
	})

	t.Run("no TLS configured leaves options and scheme alone", func(t *testing.T) {
		spice := NewSpiceClient()
		spice.flightAddress = "127.0.0.1:50051"

		options, err := spice.adbcOptions()
		if err != nil {
			t.Fatalf("adbcOptions: %v", err)
		}

		for _, key := range []string{flightsql.OptionSSLRootCerts, flightsql.OptionMTLSCertChain, flightsql.OptionMTLSPrivateKey} {
			if _, present := options[key]; present {
				t.Errorf("%s should be absent when no TLS is configured", key)
			}
		}
		if got := options[adbc.OptionKeyURI]; got != "grpc://127.0.0.1:50051" {
			t.Errorf("URI = %q, want plaintext scheme", got)
		}
	})

	t.Run("missing certificate file is reported", func(t *testing.T) {
		spice := NewSpiceClient()
		spice.flightAddress = "127.0.0.1:50051"
		spice.tlsRootCertFile = filepath.Join(dir, "does-not-exist.pem")

		if _, err := spice.adbcOptions(); err == nil {
			t.Error("expected an error naming the unreadable certificate")
		}
	})
}

var _ array.RecordReader = (*fakeRecordReader)(nil)
var _ array.RecordReader = (*leasedRecordReader)(nil)
