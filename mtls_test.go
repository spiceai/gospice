package gospice

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mtlsTestCA is a minimal self-signed CA used to issue a server and a client
// leaf certificate for exercising WithTLSClientCertificate/WithTLSRootCertificate
// end-to-end, without depending on fixture files checked into the repo.
type mtlsTestCA struct {
	certPEM    []byte
	cert       *x509.Certificate
	key        *ecdsa.PrivateKey
	nextSerial int64
}

func newMTLSTestCA(t *testing.T) *mtlsTestCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("error generating CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gospice-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("error creating CA certificate: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("error parsing CA certificate: %v", err)
	}

	return &mtlsTestCA{
		certPEM:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		cert:       cert,
		key:        key,
		nextSerial: 2,
	}
}

// issue creates a leaf certificate signed by the CA. When forServer is true, the
// certificate is valid for TLS server auth with 127.0.0.1 as a SAN; otherwise
// it's valid for TLS client auth.
func (ca *mtlsTestCA) issue(t *testing.T, commonName string, forServer bool) (certPEM, keyPEM []byte, tlsCert tls.Certificate) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("error generating leaf key: %v", err)
	}

	serial := ca.nextSerial
	ca.nextSerial++

	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if forServer {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("error creating leaf certificate: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("error marshalling leaf key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	tlsCert, err = tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("error building tls.Certificate: %v", err)
	}
	return certPEM, keyPEM, tlsCert
}

// writeTempFile writes data to a new file under t.TempDir() and returns its path.
func writeTempFile(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("error writing %s: %v", name, err)
	}
	return path
}

// newMTLSTestServer starts an httptest TLS server presenting serverCert, serving
// a canned /v1/status response, and requiring a client certificate signed by ca
// when requireClientCert is true.
func newMTLSTestServer(t *testing.T, ca *mtlsTestCA, serverCert tls.Certificate, requireClientCert bool) *httptest.Server {
	t.Helper()

	caPool := x509.NewCertPool()
	caPool.AddCert(ca.cert)

	clientAuth := tls.NoClientCert
	if requireClientCert {
		clientAuth = tls.RequireAndVerifyClientCert
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"name":"http","endpoint":"127.0.0.1:8090","status":"Ready"}]`))
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   clientAuth,
		ClientCAs:    caPool,
	}
	srv.StartTLS()
	return srv
}

// TestMTLSClientCertificate proves WithTLSClientCertificate/WithTLSRootCertificate
// actually drive a real mutual-TLS handshake, rather than just recording file paths:
// a server that requires a client certificate rejects a client that doesn't present
// one, and accepts one that does.
func TestMTLSClientCertificate(t *testing.T) {
	ca := newMTLSTestCA(t)
	_, _, serverCert := ca.issue(t, "127.0.0.1", true)
	clientCertPEM, clientKeyPEM, _ := ca.issue(t, "gospice-test-client", false)

	caCertFile := writeTempFile(t, "ca.pem", ca.certPEM)
	clientCertFile := writeTempFile(t, "client-cert.pem", clientCertPEM)
	clientKeyFile := writeTempFile(t, "client-key.pem", clientKeyPEM)

	t.Run("handshake succeeds when the client presents the required certificate", func(t *testing.T) {
		srv := newMTLSTestServer(t, ca, serverCert, true)
		defer srv.Close()

		spice := NewSpiceClient()
		defer func() { _ = spice.Close() }()

		if err := spice.Init(
			WithHttpAddress(srv.URL),
			WithTLSClientCertificate(clientCertFile, clientKeyFile),
			WithTLSRootCertificate(caCertFile),
		); err != nil {
			t.Fatalf("error initializing SpiceClient: %v", err)
		}

		got, err := spice.RuntimeStatus(context.Background())
		if err != nil {
			t.Fatalf("expected the mTLS handshake to succeed, got: %v", err)
		}
		if len(got) != 1 || got[0].Name != "http" || got[0].Status != ComponentStatusReady {
			t.Errorf("unexpected RuntimeStatus response: %+v", got)
		}
	})

	t.Run("handshake fails when the server requires a client certificate and none is configured", func(t *testing.T) {
		srv := newMTLSTestServer(t, ca, serverCert, true)
		defer srv.Close()

		spice := NewSpiceClient()
		defer func() { _ = spice.Close() }()

		if err := spice.Init(
			WithHttpAddress(srv.URL),
			WithTLSRootCertificate(caCertFile),
		); err != nil {
			t.Fatalf("error initializing SpiceClient: %v", err)
		}

		if _, err := spice.RuntimeStatus(context.Background()); err == nil {
			t.Fatal("expected the request to fail the TLS handshake without a client certificate, got nil error")
		}
	})

	t.Run("succeeds without a client certificate when the server doesn't require one", func(t *testing.T) {
		srv := newMTLSTestServer(t, ca, serverCert, false)
		defer srv.Close()

		spice := NewSpiceClient()
		defer func() { _ = spice.Close() }()

		if err := spice.Init(
			WithHttpAddress(srv.URL),
			WithTLSRootCertificate(caCertFile),
		); err != nil {
			t.Fatalf("error initializing SpiceClient: %v", err)
		}

		if _, err := spice.RuntimeStatus(context.Background()); err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
	})
}

func TestWithTLSClientCertificateValidation(t *testing.T) {
	tests := []struct {
		name     string
		certFile string
		keyFile  string
	}{
		{name: "empty cert file", certFile: "", keyFile: "key.pem"},
		{name: "empty key file", certFile: "cert.pem", keyFile: ""},
		{name: "both empty", certFile: "", keyFile: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &SpiceClient{}
			if err := WithTLSClientCertificate(tt.certFile, tt.keyFile)(c); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}

func TestWithTLSRootCertificateValidation(t *testing.T) {
	c := &SpiceClient{}
	if err := WithTLSRootCertificate("")(c); err == nil {
		t.Fatal("expected an error for an empty CA file, got nil")
	}
}

func TestInitFailsOnMissingTLSFiles(t *testing.T) {
	t.Run("missing client certificate files", func(t *testing.T) {
		spice := NewSpiceClient()
		defer func() { _ = spice.Close() }()

		err := spice.Init(WithTLSClientCertificate("/nonexistent/cert.pem", "/nonexistent/key.pem"))
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if !strings.Contains(err.Error(), "mTLS") {
			t.Errorf("error %q does not mention mTLS", err.Error())
		}
	})

	t.Run("missing CA certificate file", func(t *testing.T) {
		spice := NewSpiceClient()
		defer func() { _ = spice.Close() }()

		err := spice.Init(WithTLSRootCertificate("/nonexistent/ca.pem"))
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if !strings.Contains(err.Error(), "TLS root certificate") {
			t.Errorf("error %q does not mention the TLS root certificate", err.Error())
		}
	})
}
