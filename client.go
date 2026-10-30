package gospice

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	MAX_MESSAGE_SIZE_BYTES = 100 * 1024 * 1024
)

var defaultCloudConfig = LoadConfig()
var defaultLocalConfig = LoadLocalConfig()

// SpiceClient is a client for Spice.ai OSS, a unified SQL query interface and portable runtime to
// locally materialize, accelerate, and query datasets across databases, data warehouses, and data lakes.
//
// https://spiceai.org
// For documentation visit https://docs.spiceai.org/sdks/golang
type SpiceClient struct {
	appId         string
	apiKey        string
	flightAddress string
	baseHttpUrl   string

	flightClient flight.Client
	sessionMu    sync.Mutex // guards sessionToken and handshakeFlight
	// handshakeFlight is the handshake in progress, if any. Callers that arrive
	// while one is running wait for its outcome instead of starting their own.
	handshakeFlight *handshakeFlight
	// sessionToken is the bearer token the runtime issued for apiKey on the
	// last handshake, reused by every authenticated Flight call until the
	// runtime stops recognising it; empty when no session is established.
	sessionToken  string
	adbcClient    *ADBCClient
	adbcMu        sync.Mutex // guards re-initialization of adbcClient
	httpClient    http.Client
	backoffPolicy backoff.BackOff
	maxRetries    uint
	userAgent     string

	tlsClientCertFile string
	tlsClientKeyFile  string
	tlsRootCertFile   string

	// httpAddressSet records that WithHttpAddress named an endpoint, so Init
	// leaves it alone instead of deriving one from the Flight address.
	httpAddressSet bool

	// cloudAuth treats the Flight address as Spice Cloud for authentication,
	// so a Cloud key uses the Basic handshake. Set only by tests that stand in
	// for Cloud with a local server.
	cloudAuth bool
}

// NewSpiceClient creates a new SpiceClient
func NewSpiceClient() *SpiceClient {
	return NewSpiceClientWithAddress(defaultLocalConfig.FlightUrl)
}

func NewSpiceClientWithAddress(flightAddress string) *SpiceClient {
	spiceClient := &SpiceClient{
		flightAddress: flightAddress,
		baseHttpUrl:   httpUrlFor(flightAddress),
		httpClient: http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				DisableCompression:  false,
			},
		},
		maxRetries: 3,
	}
	spiceClient.backoffPolicy = spiceClient.getBackoffPolicy()
	spiceClient.userAgent = GetSpiceUserAgent()
	return spiceClient
}

type SpiceClientModifier func(c *SpiceClient) error

func WithApiKey(apiKey string) SpiceClientModifier {
	return func(c *SpiceClient) error {
		if apiKey == "" {
			return fmt.Errorf("apiKey is required")
		}
		// Spice Cloud keys have the form "<appId>|<secret>" and authenticate
		// with a Basic handshake. Any other non-empty key is a plain key, as
		// configured on a Spice OSS runtime, and is sent as a Bearer token.
		c.appId = ""
		if appId, secret, ok := strings.Cut(apiKey, "|"); ok && appId != "" && secret != "" && !strings.Contains(secret, "|") {
			c.appId = appId
		}
		c.apiKey = apiKey

		return nil
	}
}

// useBearerAuth reports whether the API key is sent directly as an
// "Authorization: Bearer <key>" header rather than exchanged for a session
// through the Flight Basic handshake. Spice OSS runtimes secured with an API
// key accept only the Bearer form, so it is used for plain keys (no appId) and
// for any address that is not Spice Cloud. Cloud keys sent to Spice Cloud keep
// the Basic handshake.
func (c *SpiceClient) useBearerAuth() bool {
	if c.apiKey == "" {
		return false
	}
	return c.appId == "" || (!c.cloudAuth && !isCloudAddress(c.flightAddress))
}

// isCloudAddress reports whether address points at Spice Cloud.
func isCloudAddress(address string) bool {
	if address == defaultCloudConfig.FlightUrl {
		return true
	}
	host := address
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	if i := strings.IndexAny(host, ":/"); i >= 0 {
		host = host[:i]
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	return host == "spiceai.io" || strings.HasSuffix(host, ".spiceai.io")
}

func WithFlightAddress(address string) SpiceClientModifier {
	return func(c *SpiceClient) error {
		c.flightAddress = address
		return nil
	}
}

func WithHttpAddress(address string) SpiceClientModifier {
	return func(c *SpiceClient) error {
		c.baseHttpUrl = address
		c.httpAddressSet = true
		return nil
	}
}

func WithUserAgent(userAgent string) SpiceClientModifier {
	return func(c *SpiceClient) error {
		// Prepend the user agent with the user-provided string
		c.userAgent = RemoveNonPrintableASCII(userAgent) + " " + c.userAgent
		return nil
	}
}

func WithSpiceCloudAddress() SpiceClientModifier {
	return func(c *SpiceClient) error {
		c.flightAddress = defaultCloudConfig.FlightUrl
		return nil
	}
}

// WithTLSClientCertificate configures the client to present a client certificate
// during the TLS handshake for mutual TLS (mTLS) authentication.
// Both certFile and keyFile must be PEM-encoded.
func WithTLSClientCertificate(certFile, keyFile string) SpiceClientModifier {
	return func(c *SpiceClient) error {
		if certFile == "" || keyFile == "" {
			return fmt.Errorf("both certFile and keyFile are required for mTLS")
		}
		c.tlsClientCertFile = certFile
		c.tlsClientKeyFile = keyFile
		return nil
	}
}

// WithTLSRootCertificate configures the client to use a custom CA certificate
// file for server verification instead of (in addition to) the system certificate store.
func WithTLSRootCertificate(caFile string) SpiceClientModifier {
	return func(c *SpiceClient) error {
		if caFile == "" {
			return fmt.Errorf("caFile is required")
		}
		c.tlsRootCertFile = caFile
		return nil
	}
}

// Init initializes the SpiceClient
func (c *SpiceClient) Init(opts ...SpiceClientModifier) error {
	for _, opt := range opts {
		err := opt(c)
		if err != nil {
			return err
		}
	}

	// Options may have moved the Flight endpoint (WithFlightAddress,
	// WithSpiceCloudAddress). Unless the caller named an HTTP endpoint of their
	// own, follow it, so both halves of the client address the same runtime and
	// the API key is only ever sent to a host the caller configured.
	if !c.httpAddressSet {
		c.baseHttpUrl = httpUrlFor(c.flightAddress)
	}

	systemCertPool, err := x509.SystemCertPool()
	if err != nil {
		return fmt.Errorf("error getting system cert pool: %w", err)
	}

	if c.tlsRootCertFile != "" {
		caPem, err := os.ReadFile(c.tlsRootCertFile)
		if err != nil {
			return fmt.Errorf("error reading TLS root certificate '%s': %w", c.tlsRootCertFile, err)
		}
		if !systemCertPool.AppendCertsFromPEM(caPem) {
			return fmt.Errorf("failed to append CA certificate from '%s'", c.tlsRootCertFile)
		}
	}

	flightClient, err := c.createClient(c.flightAddress, systemCertPool)
	if err != nil {
		return fmt.Errorf("error creating Spice Flight client: %w", err)
	}

	c.flightClient = flightClient
	// A session belongs to the connection that performed its handshake.
	c.forgetSession("")

	// Update the HTTP client transport with the same TLS configuration
	// (custom CA and/or client certificate) used by the Flight client.
	httpTlsConfig := &tls.Config{
		RootCAs:    systemCertPool,
		MinVersion: tls.VersionTLS12,
	}
	if c.tlsClientCertFile != "" && c.tlsClientKeyFile != "" {
		clientCert, err := tls.LoadX509KeyPair(c.tlsClientCertFile, c.tlsClientKeyFile)
		if err != nil {
			return fmt.Errorf("error loading client certificate for HTTP mTLS: %w", err)
		}
		httpTlsConfig.Certificates = []tls.Certificate{clientCert}
	}
	c.httpClient.Transport = &http.Transport{
		MaxIdleConnsPerHost: 10,
		DisableCompression:  false,
		TLSClientConfig:     httpTlsConfig,
	}

	// Initialize ADBC client - non-fatal, will be initialized lazily if needed
	// This allows health checks to work even if ADBC connection fails initially
	_ = c.initADBC()

	return nil
}

// Sets the maximum number of times to retry Query and FireQuery calls.
// The default is 3. Setting to 0 will disable retries.
func (c *SpiceClient) SetMaxRetries(maxRetries uint) {
	c.maxRetries = maxRetries
}

// withRetries runs op, retrying transient failures up to maxRetries times.
// When maxRetries is 0, retries are disabled: op runs once and its error is
// returned as-is. grpc-go rejects a retryPolicy with MaxAttempts < 2, so
// createClient also omits the Flight service-config retryPolicy in that case.
func (c *SpiceClient) withRetries(op func() error) error {
	if c.maxRetries == 0 {
		err := op()
		// backoff.Retry unwraps *PermanentError so callers see the inner error.
		// The single-attempt path must do the same: Sql and execADBCWithBackoff
		// mark non-retryable failures with backoff.Permanent.
		var permanent *backoff.PermanentError
		if errors.As(err, &permanent) {
			return permanent.Err
		}
		return err
	}
	return backoff.Retry(op, backoff.WithMaxRetries(c.backoffPolicy, uint64(c.maxRetries)))
}

// flightServiceConfig is the gRPC default service config for the Flight client.
// An empty string means no service config: the caller asked for zero retries.
func (c *SpiceClient) flightServiceConfig() string {
	if c.maxRetries == 0 {
		return ""
	}
	return fmt.Sprintf(`{
		"methodConfig": [{
	        "name": [{"service": "arrow.flight.protocol.FlightService"}],
	        "waitForReady": true,
	        "retryPolicy": {
	            "MaxAttempts": %d,
	            "InitialBackoff": "0.1s",
	            "MaxBackoff": "0.225s",
	            "BackoffMultiplier": 1.5,
				"RetryableStatusCodes": [ "UNAVAILABLE", "UNKNOWN", "INTERNAL" ]
	        }
	    }]
	}`, c.maxRetries+1)
}

// Close closes the SpiceClient and cleans up resources
func (c *SpiceClient) Close() error {
	var errors []error

	if c.flightClient != nil {
		err := c.flightClient.Close()
		if err != nil {
			errors = append(errors, err)
		}
	}
	c.forgetSession("")

	if err := c.closeADBC(); err != nil {
		errors = append(errors, err)
	}

	c.httpClient.CloseIdleConnections()

	if len(errors) > 0 {
		return fmt.Errorf("error closing SpiceClient: %v", errors)
	}

	return nil
}

func FlightHeadersInterceptor(headers map[string]string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		// ensure existing headers are retained
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(headers)
		}

		// add new headers
		for k, v := range headers {
			md[k] = append(md[k], v)
		}

		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// httpUrlFor returns the HTTP endpoint a client on flightAddress uses when no
// WithHttpAddress was given.
//
// The two default Flight endpoints keep their known pairing, and any other
// Spice Cloud Flight host uses the Spice Cloud HTTP endpoint. An HTTP endpoint
// set explicitly through SPICE_HTTP_URL is kept for every other address, as
// before. Otherwise a self-hosted runtime on a custom host or port is paired
// with the runtime's default HTTP port (8090) on the same host, over HTTPS
// unless the Flight address is plaintext (grpc://). A client on a custom Flight
// address therefore never sends its HTTP calls, or the API key it was
// configured with, to Spice Cloud or to any host the caller did not name.
//
// An address whose application host cannot be read — a unix socket, a resolver
// scheme this client does not know — yields "", so HTTP calls fail rather than
// reach a host the caller did not configure; WithHttpAddress fixes it.
func httpUrlFor(flightAddress string) string {
	if paired, ok := defaultHttpUrlFor(flightAddress); ok {
		return paired
	}
	host, tls, ok := flightTargetHost(flightAddress)
	if ok && isCloudHost(host) {
		return defaultCloudConfig.HttpUrl
	}
	if defaultCloudConfig.httpUrlFromEnv {
		return defaultCloudConfig.HttpUrl
	}
	if !ok {
		return ""
	}
	scheme := "http"
	if tls {
		scheme = "https"
	}
	return scheme + "://" + net.JoinHostPort(host, defaultRuntimeHttpPort)
}

// defaultRuntimeHttpPort is the port the Spice runtime serves its HTTP API on
// by default.
const defaultRuntimeHttpPort = "8090"

// flightTargetHost returns the application host a Flight address dials, and
// whether that connection uses TLS (createClient uses plaintext only for
// grpc://). It understands the forms createClient and gRPC accept: a bare
// host[:port], grpc://, grpc+tcp:// and grpc+tls:// addresses, and the dns: and
// passthrough: resolver targets, whose application endpoint is the path after
// the authority. A dns:// target with an explicit resolver authority reports
// false, as does anything else.
func flightTargetHost(address string) (host string, tls bool, ok bool) {
	tls = true
	// Opaque resolver targets (dns:host:port, passthrough:host:port) name the
	// application endpoint directly.
	if i := strings.Index(address, ":"); i >= 0 && !strings.Contains(address, "://") {
		switch strings.ToLower(address[:i]) {
		case "dns", "passthrough":
			address = strings.TrimPrefix(address[i+1:], "/")
		}
	}
	authority := address
	if i := strings.Index(address, "://"); i >= 0 {
		scheme := strings.ToLower(address[:i])
		rest := address[i+3:]
		switch scheme {
		case "grpc", "grpc+tcp":
			tls = false
			authority = rest
		case "grpc+tls":
			authority = rest
		case "dns", "passthrough":
			slash := strings.Index(rest, "/")
			if slash < 0 {
				return "", false, false
			}
			// dns://resolver/host names a resolver the HTTP client does not use;
			// a host derived from it could resolve to a different deployment, so
			// such a target needs WithHttpAddress.
			if scheme == "dns" && slash > 0 {
				return "", false, false
			}
			authority = rest[slash+1:]
		default:
			return "", false, false
		}
		if j := strings.IndexAny(authority, "/?#"); j >= 0 {
			authority = authority[:j]
		}
	} else if strings.ContainsAny(address, "/?#") {
		return "", false, false
	}

	host = authority
	if h, port, err := net.SplitHostPort(authority); err == nil {
		if port == "" {
			return "", false, false
		}
		host = h
	} else if strings.Contains(authority, ":") && !strings.HasPrefix(authority, "[") {
		// "scheme:opaque" targets such as unix:path have no host this client can name.
		return "", false, false
	} else {
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}
	if host == "" || strings.ContainsAny(host, "@ ") {
		return "", false, false
	}
	return host, tls, true
}

// isCloudHost reports whether host is a Spice Cloud host.
func isCloudHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	return host == "spiceai.io" || strings.HasSuffix(host, ".spiceai.io")
}

// defaultHttpUrlFor returns the HTTP endpoint that belongs with a Flight
// endpoint, and whether that pairing is known at all. The runtime serves Flight
// and HTTP as one deployment, so a client querying a local runtime has to reach
// that runtime's HTTP API too — pairing a local Flight address with the Cloud
// HTTP endpoint means health checks, dataset refreshes, search and NSQL all
// report on a runtime the caller never named.
//
// Only the two default endpoints have a counterpart this function names; see
// httpUrlFor for every other address.
//
// A Flight endpoint moved by SPICE_FLIGHT_URL or SPICE_LOCAL_FLIGHT_URL pairs
// with its HTTP default only when the matching HTTP variable was set too;
// otherwise the built-in HTTP default would name a runtime the caller moved
// away from (Spice Cloud, or this machine), so it is treated as custom.
func defaultHttpUrlFor(flightAddress string) (string, bool) {
	switch flightAddress {
	case defaultCloudConfig.FlightUrl:
		if defaultCloudConfig.pairingConfigured() {
			return defaultCloudConfig.HttpUrl, true
		}
	case defaultLocalConfig.FlightUrl:
		if defaultLocalConfig.pairingConfigured() {
			return defaultLocalConfig.HttpUrl, true
		}
	}
	return "", false
}

// flightTransport returns the target to dial for a Flight address and whether
// the connection uses TLS. It is the one transport policy for this client:
// Sql (native Flight) and SqlWithParams (ADBC FlightSQL) both use it, so the
// same address never means TLS on one path and plaintext on the other.
//
// Only an explicit grpc:// prefix dials without TLS. A grpc+tls:// address has
// its scheme removed and uses TLS. Every other address, a bare host:port
// included, uses TLS whatever its host or port: the decision never depends on
// whether the host looks like Spice Cloud, so a misspelled or unexpected host
// fails a TLS handshake instead of falling back to plaintext.
func flightTransport(address string) (target string, useTLS bool) {
	if rest, ok := strings.CutPrefix(address, "grpc://"); ok {
		return rest, false
	}
	if rest, ok := strings.CutPrefix(address, "grpc+tls://"); ok {
		return rest, true
	}
	return address, true
}

func (c *SpiceClient) createClient(address string, systemCertPool *x509.CertPool) (flight.Client, error) {
	grpcDialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MAX_MESSAGE_SIZE_BYTES),
			grpc.MaxCallSendMsgSize(MAX_MESSAGE_SIZE_BYTES),
		),
		grpc.WithUnaryInterceptor(FlightHeadersInterceptor(map[string]string{
			"user-agent": c.userAgent,
		})),
	}
	if cfg := c.flightServiceConfig(); cfg != "" {
		grpcDialOpts = append(grpcDialOpts, grpc.WithDefaultServiceConfig(cfg))
	}

	address, useTLS := flightTransport(address)
	if !useTLS {
		grpcDialOpts = append(grpcDialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsConfig := &tls.Config{
			RootCAs:    systemCertPool,
			MinVersion: tls.VersionTLS12,
		}

		if c.tlsClientCertFile != "" && c.tlsClientKeyFile != "" {
			clientCert, err := tls.LoadX509KeyPair(c.tlsClientCertFile, c.tlsClientKeyFile)
			if err != nil {
				return nil, fmt.Errorf("error loading client certificate for mTLS: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{clientCert}
		}

		grpcDialOpts = append(grpcDialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	client, err := flight.NewClientWithMiddleware(
		address,
		nil,
		nil,
		grpcDialOpts...,
	)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *SpiceClient) getBackoffPolicy() backoff.BackOff {
	initialInterval := 250 * time.Millisecond
	maxInterval := initialInterval * time.Duration(math.Ceil(float64(c.maxRetries)*backoff.DefaultMultiplier))
	maxElapsedTime := maxInterval * time.Duration(c.maxRetries)
	b := &backoff.ExponentialBackOff{
		InitialInterval:     initialInterval,
		RandomizationFactor: backoff.DefaultRandomizationFactor, // 0.5
		Multiplier:          backoff.DefaultMultiplier,          // 1.5
		MaxInterval:         maxInterval,
		MaxElapsedTime:      maxElapsedTime,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	return b
}

// IsSpiceHealthy checks if the Spice instance is healthy by calling the /health endpoint.
// This is an unauthenticated endpoint that returns 200 OK with "ok" in the body if the instance is healthy.
// Returns true if healthy, false otherwise.
func (c *SpiceClient) IsSpiceHealthy(ctx context.Context) bool {
	healthUrl := fmt.Sprintf("%s/health", c.baseHttpUrl)

	req, err := http.NewRequestWithContext(ctx, "GET", healthUrl, nil)
	if err != nil {
		return false
	}

	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer func() {
		_ = resp.Body.Close() // Ignore close errors in health check
	}()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	// Read and check body for "ok"
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	return strings.Contains(strings.ToLower(string(body)), "ok")
}

// IsSpiceReady checks if the Spice instance is ready to accept queries by calling the /v1/ready endpoint.
// For Spice Cloud, this endpoint requires authentication via API key. For local Spice instances, no API key is required.
// Returns "ready" in the body when ready. Returns true if ready, false otherwise.
func (c *SpiceClient) IsSpiceReady(ctx context.Context) bool {
	readyUrl := fmt.Sprintf("%s/v1/ready", c.baseHttpUrl)

	req, err := http.NewRequestWithContext(ctx, "GET", readyUrl, nil)
	if err != nil {
		return false
	}

	req.Header.Set("User-Agent", c.userAgent)
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer func() {
		_ = resp.Body.Close() // Ignore close errors in ready check
	}()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	// Read and check body for "ready"
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	return strings.Contains(strings.ToLower(string(body)), "ready")
}
