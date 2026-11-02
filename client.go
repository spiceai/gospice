package gospice

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	sessionToken string
	adbcClient   *ADBCClient
	adbcMu       sync.Mutex // guards re-initialization of adbcClient
	httpClient   http.Client
	// maxRetries is read on every call, so SetMaxRetries takes effect whether
	// it is called before or after Init, and concurrently with queries.
	maxRetries atomic.Uint64
	userAgent  string

	// waitForReady makes native Flight calls wait for the connection to become
	// ready instead of failing fast while the runtime is unreachable. Off by
	// default; see WithWaitForReady.
	waitForReady bool

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
	// An address with no known HTTP counterpart keeps the Cloud endpoint this
	// constructor has always defaulted to; WithHttpAddress names the runtime's own.
	baseHttpUrl := defaultCloudConfig.HttpUrl
	if paired, ok := defaultHttpUrlFor(flightAddress); ok {
		baseHttpUrl = paired
	}
	spiceClient := &SpiceClient{
		flightAddress: flightAddress,
		baseHttpUrl:   baseHttpUrl,
		httpClient: http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				DisableCompression:  false,
			},
		},
	}
	spiceClient.maxRetries.Store(defaultMaxRetries)
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
	// own, follow it, so both halves of the client address the same runtime.
	// Resolved from the Flight address the options settled on, so an address
	// with no known counterpart falls back to the Cloud endpoint this client has
	// always defaulted to rather than keeping one derived for a different runtime.
	if !c.httpAddressSet {
		if paired, ok := defaultHttpUrlFor(c.flightAddress); ok {
			c.baseHttpUrl = paired
		} else {
			c.baseHttpUrl = defaultCloudConfig.HttpUrl
		}
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

// defaultMaxRetries is the number of retries a new client makes for a
// transient failure, on top of the first attempt.
const defaultMaxRetries = 3

// SetMaxRetries sets the maximum number of times Sql, SqlWithParams and Query
// retry a transient failure (Unavailable, Internal and similar) after the
// first attempt. The default is 3. Setting it to 0 disables retries.
//
// It takes effect on the next call, whether it is called before or after Init.
func (c *SpiceClient) SetMaxRetries(maxRetries uint) {
	c.maxRetries.Store(uint64(maxRetries))
}

// WithWaitForReady makes native Flight calls (Sql, Query and the async query
// actions) wait for the connection to the runtime to become ready, instead of
// failing with codes.Unavailable while it cannot be reached. A call then
// blocks until the runtime is reachable or ctx ends, so pass a ctx with a
// deadline: with context.Background() a call against a runtime that never
// comes up never returns.
//
// It is off by default: an unreachable runtime fails each attempt fast with
// Unavailable, and the call returns once SetMaxRetries retries are spent.
// Use it for a client that starts before its runtime and should wait for it.
// SqlWithParams (ADBC) does not use it.
func WithWaitForReady(enabled bool) SpiceClientModifier {
	return func(c *SpiceClient) error {
		c.waitForReady = enabled
		return nil
	}
}

// withRetries runs op, retrying transient failures up to the current
// maxRetries times with exponential backoff, and stops early once ctx ends.
// When maxRetries is 0, retries are disabled: op runs once and its error is
// returned as-is. Every call gets its own backoff, sized from maxRetries at
// the time of the call.
//
// This is the only retry layer for Flight calls. The gRPC connection carries
// no retryPolicy service config, which would be fixed at Init, would multiply
// the attempts made here, and, combined with waitForReady, never let an
// attempt fail while the runtime is unreachable.
func (c *SpiceClient) withRetries(ctx context.Context, op func() error) error {
	maxRetries := c.maxRetries.Load()
	if maxRetries == 0 {
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
	b := backoff.WithContext(backoff.WithMaxRetries(newBackoffPolicy(maxRetries), maxRetries), ctx)
	var lastErr error
	err := backoff.Retry(func() error {
		lastErr = op()
		return lastErr
	}, b)
	// When ctx ends between attempts, backoff reports the bare context error.
	// The last attempt's error says why the call failed (an rpc status naming
	// the connection error), so return that instead.
	if err != nil && lastErr != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		var permanent *backoff.PermanentError
		if errors.As(lastErr, &permanent) {
			return permanent.Err
		}
		return lastErr
	}
	return err
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

// defaultHttpUrlFor returns the HTTP endpoint that belongs with a Flight
// endpoint, and whether that pairing is known at all. The runtime serves Flight
// and HTTP as one deployment, so a client querying a local runtime has to reach
// that runtime's HTTP API too — pairing a local Flight address with the Cloud
// HTTP endpoint means health checks, dataset refreshes, search and NSQL all
// report on a runtime the caller never named.
//
// Only the two default endpoints have a counterpart this client can name. Any
// other Flight address — a self-hosted runtime, one behind a proxy — may serve
// its HTTP API anywhere, so it reports false and the caller keeps the HTTP
// endpoint it already had rather than being pointed at this machine.
func defaultHttpUrlFor(flightAddress string) (string, bool) {
	switch flightAddress {
	case defaultCloudConfig.FlightUrl:
		return defaultCloudConfig.HttpUrl, true
	case defaultLocalConfig.FlightUrl:
		return defaultLocalConfig.HttpUrl, true
	default:
		return "", false
	}
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
	if c.waitForReady {
		grpcDialOpts = append(grpcDialOpts, grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
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

// newBackoffPolicy returns the backoff between retries for a call that makes
// up to maxRetries retries.
func newBackoffPolicy(maxRetries uint64) backoff.BackOff {
	initialInterval := 250 * time.Millisecond
	maxInterval := initialInterval * time.Duration(math.Ceil(float64(maxRetries)*backoff.DefaultMultiplier))
	maxElapsedTime := maxInterval * time.Duration(maxRetries)
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
