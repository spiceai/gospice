package gospice

import (
	"context"
	"crypto/x509"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/apache/arrow/go/v16/arrow/flight"
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

// SpiceClient is a client for Spice.ai - Data and AI infrastructure for web3
// https://spice.ai
// For documentation visit https://docs.spice.ai/sdks/go-sdk
type SpiceClient struct {
	appId         string
	apiKey        string
	flightAddress string
	baseHttpUrl   string

	flightClient  flight.Client
	httpClient    http.Client
	backoffPolicy backoff.BackOff
	maxRetries    uint
	userAgent     string
}

// NewSpiceClient creates a new SpiceClient
func NewSpiceClient() *SpiceClient {
	return NewSpiceClientWithAddress(defaultLocalConfig.FlightUrl)
}

func NewSpiceClientWithAddress(flightAddress string) *SpiceClient {
	spiceClient := &SpiceClient{
		flightAddress: flightAddress,
		baseHttpUrl:   defaultCloudConfig.HttpUrl,
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
		apiKeyParts := strings.Split(apiKey, "|")
		if len(apiKeyParts) != 2 {
			return fmt.Errorf("apiKey is invalid")
		}

		c.appId = apiKeyParts[0]
		c.apiKey = apiKey

		return nil
	}
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
		return nil
	}
}

func WithSpiceCloudAddress() SpiceClientModifier {
	return func(c *SpiceClient) error {
		c.flightAddress = defaultCloudConfig.FlightUrl
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

	systemCertPool, err := x509.SystemCertPool()
	if err != nil {
		return fmt.Errorf("error getting system cert pool: %w", err)
	}

	flightClient, err := c.createClient(c.flightAddress, systemCertPool)
	if err != nil {
		return fmt.Errorf("error creating Spice Flight client: %w", err)
	}

	c.flightClient = flightClient

	return nil
}

// Sets the maximum number of times to retry Query and FireQuery calls.
// The default is 3. Setting to 0 will disable retries.
func (c *SpiceClient) SetMaxRetries(maxRetries uint) {
	c.maxRetries = maxRetries
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

func (c *SpiceClient) createClient(address string, systemCertPool *x509.CertPool) (flight.Client, error) {
	retryPolicy := fmt.Sprintf(`{
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
	grpcDialOpts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(retryPolicy),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MAX_MESSAGE_SIZE_BYTES),
			grpc.MaxCallSendMsgSize(MAX_MESSAGE_SIZE_BYTES),
		),
		grpc.WithUnaryInterceptor(FlightHeadersInterceptor(map[string]string{
			"x-spice-user-agent": c.userAgent,
		})),
	}

	if strings.HasPrefix(address, "grpc://") {
		address = strings.TrimPrefix(address, "grpc://")
		grpcDialOpts = append(grpcDialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		grpcDialOpts = append(grpcDialOpts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(systemCertPool, "")))
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
