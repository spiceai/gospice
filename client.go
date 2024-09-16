package gospice

import (
	"context"
	"crypto/x509"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
	appId            string
	apiKey           string
	flightAddress    string
	firecacheAddress string
	baseHttpUrl      string

	flightClient    flight.Client
	firecacheClient flight.Client
	httpClient      http.Client
	backoffPolicy   backoff.BackOff
	maxRetries      uint
}

// NewSpiceClient creates a new SpiceClient
func NewSpiceClient() *SpiceClient {
	return NewSpiceClientWithAddress(defaultLocalConfig.FlightUrl, defaultLocalConfig.FirecacheUrl)
}

func NewSpiceClientWithAddress(flightAddress string, firecacheAddress string) *SpiceClient {
	spiceClient := &SpiceClient{
		flightAddress:    flightAddress,
		firecacheAddress: firecacheAddress,
		baseHttpUrl:      defaultCloudConfig.HttpUrl,
		httpClient: http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				DisableCompression:  false,
			},
		},
		maxRetries: 3,
	}
	spiceClient.backoffPolicy = spiceClient.getBackoffPolicy()
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

func WithFirecacheAddress(address string) SpiceClientModifier {
	return func(c *SpiceClient) error {
		c.firecacheAddress = address
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
		c.firecacheAddress = defaultCloudConfig.FirecacheUrl
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

	firecacheClient, err := c.createClient(c.firecacheAddress, systemCertPool)
	if err != nil {
		return fmt.Errorf("error creating Spice Firecache client: %w", err)
	}

	c.flightClient = flightClient
	c.firecacheClient = firecacheClient

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
	if c.firecacheClient != nil {
		err := c.firecacheClient.Close()
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

func (c *SpiceClient) query(ctx context.Context, client flight.Client, appId string, apiKey string, sql string) (array.RecordReader, error) {
	var rdr array.RecordReader
	err := backoff.Retry(func() error {
		var err error
		rdr, err = c.queryInternal(ctx, client, appId, apiKey, sql)
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				switch st.Code() {
				case codes.Unavailable, codes.Unknown, codes.DeadlineExceeded, codes.Aborted, codes.Internal:
					return err
				}
				if strings.Contains(err.Error(), "malformed header: missing HTTP content-type") {
					return err
				}
				if err.Error() == "rpc error: code = Unknown desc = " {
					return err
				}
			}
			return backoff.Permanent(err)
		}
		return nil
	}, backoff.WithMaxRetries(c.backoffPolicy, uint64(c.maxRetries)))
	if err != nil {
		return nil, err
	}

	return rdr, nil
}

func (c *SpiceClient) queryInternal(ctx context.Context, client flight.Client, appId string, apiKey string, sql string) (array.RecordReader, error) {
	if client == nil {
		return nil, fmt.Errorf("flight client is not initialized")
	}

	authContext, err := client.AuthenticateBasicToken(ctx, appId, apiKey)
	if err != nil {
		return nil, err
	}

	fd := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(sql),
	}

	info, err := client.GetFlightInfo(authContext, fd)
	if err != nil {
		return nil, err
	}

	stream, err := client.DoGet(authContext, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, err
	}

	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, err
	}

	return rdr, nil
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
