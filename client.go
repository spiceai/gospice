package gospice

import (
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"

	"github.com/apache/arrow/go/v11/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	MAX_MESSAGE_SIZE_BYTES = 100 * 1024 * 1024
)

// SpiceClient is a client for Spice.xyz - Data and AI infrastructure for web3
// https://spice.xyz
// For documentation visit https://docs.spice.xyz/sdks/go-sdk
type SpiceClient struct {
	appId   string
	apiKey  string
	address string

	flightClient flight.Client
	httpClient   http.Client
}

// NewSpiceClient creates a new SpiceClient
func NewSpiceClient() *SpiceClient {
	return NewSpiceClientWithAddress("flight.spiceai.io:443")
}

func NewSpiceClientWithAddress(address string) *SpiceClient {
	return &SpiceClient{
		address: address,
		httpClient: http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				DisableCompression:  false,
			},
		},
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

	grpcDialOpts := []grpc.DialOption{grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(MAX_MESSAGE_SIZE_BYTES),
		grpc.MaxCallSendMsgSize(MAX_MESSAGE_SIZE_BYTES))}

	if strings.HasPrefix("grpc://", c.address) {
		grpcDialOpts = append(grpcDialOpts, grpc.WithInsecure())
	} else {
		grpcDialOpts = append(grpcDialOpts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(systemCertPool, "")))
	}

	// Creating flightClient connected to Spice
	flightClient, err := flight.NewClientWithMiddleware(
		c.address,
		nil,
		nil,
		grpcDialOpts,
	)
	if err != nil {
		return fmt.Errorf("error creating Spice Flight client: %w", err)
	}

	c.appId = apiKeyParts[0]
	c.apiKey = apiKey
	c.flightClient = flightClient

	return nil
}

// Close closes the SpiceClient and cleans up resources
func (c *SpiceClient) Close() error {
	if c.flightClient != nil {
		return c.flightClient.Close()
	}
	c.httpClient.CloseIdleConnections()

	return nil
}
