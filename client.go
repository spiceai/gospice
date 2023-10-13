package gospice

import (
	"context"
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	MAX_MESSAGE_SIZE_BYTES = 100 * 1024 * 1024
)

// SpiceClient is a client for Spice.xyz - Data and AI infrastructure for web3
// https://spice.xyz
// For documentation visit https://docs.spice.xyz/sdks/go-sdk
type SpiceClient struct {
	appId            string
	apiKey           string
	flightAddress    string
	firecacheAddress string

	flightClient    flight.Client
	firecacheClient flight.Client
	httpClient      http.Client
	maxRetries      uint
}

// NewSpiceClient creates a new SpiceClient
func NewSpiceClient() *SpiceClient {
	return NewSpiceClientWithAddress("flight.spiceai.io:443", "firecache.spiceai.io:443")
}

func NewSpiceClientWithAddress(flightAddress string, firecacheAddress string) *SpiceClient {
	return &SpiceClient{
		flightAddress:    flightAddress,
		firecacheAddress: firecacheAddress,
		httpClient: http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				DisableCompression:  false,
			},
		},
		maxRetries: 3,
	}
}

// Init initializes the SpiceClient
func (c *SpiceClient) Init(apiKey string) error {
	if apiKey == "" {
		return fmt.Errorf("apiKey is required")
	}
	apiKeyParts := strings.Split(apiKey, "|")
	if len(apiKeyParts) != 2 {
		return fmt.Errorf("apiKey is invalid")
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

	c.appId = apiKeyParts[0]
	c.apiKey = apiKey
	c.flightClient = flightClient
	c.firecacheClient = firecacheClient

	return nil
}

// Sets the maximum number of times to retry Query and FireQuery calls.
// The default is 3. Setting to 1 will disable retries.
func (c *SpiceClient) SetMaxRetries(maxRetries uint) {
	if maxRetries < 1 {
		maxRetries = 1
	}
	c.maxRetries = maxRetries
}

// Close closes the SpiceClient and cleans up resources
func (c *SpiceClient) Close() error {
	if c.flightClient != nil {
		return c.flightClient.Close()
	}
	if c.firecacheClient != nil {
		return c.firecacheClient.Close()
	}
	c.httpClient.CloseIdleConnections()

	return nil
}

func (c *SpiceClient) query(ctx context.Context, client flight.Client, appId string, apiKey string, sql string) (array.RecordReader, error) {
	if client == nil {
		return nil, fmt.Errorf("Flight Client is not initialized")
	}

	var authContext context.Context
	var err error
	for i := uint(0); i < c.maxRetries; i++ {
		authContext, err = client.AuthenticateBasicToken(ctx, appId, apiKey)
		if err == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("error authenticating with Spice.xyz: %s", err)
	}

	fd := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(sql),
	}

	var info *flight.FlightInfo
	for i := uint(0); i < c.maxRetries; i++ {
		info, err = client.GetFlightInfo(authContext, fd)
		if err == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err != nil {
		return nil, err
	}

	var stream flight.FlightService_DoGetClient
	var rdr *flight.Reader
	for i := uint(0); i < c.maxRetries; i++ {
		stream, err = client.DoGet(authContext, info.Endpoint[0].Ticket)
		if err != nil {
			time.Sleep(250 * time.Millisecond)
			continue
		}

		rdr, err = flight.NewRecordReader(stream)
		if err == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err != nil {
		return nil, err
	}

	return rdr, err
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
	}`, c.maxRetries)
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
