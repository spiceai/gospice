package gospice

import (
	"context"
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"

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

	systemCertPool *x509.CertPool
	httpClient     http.Client
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

	var err error
	c.systemCertPool, err = x509.SystemCertPool()
	if err != nil {
		return fmt.Errorf("error getting system cert pool: %w", err)
	}

	c.appId = apiKeyParts[0]
	c.apiKey = apiKey
	return nil
}

// Close closes the SpiceClient and cleans up resources
func (c *SpiceClient) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

func (c *SpiceClient) query(ctx context.Context, address string, appId string, apiKey string, sql string) (array.RecordReader, error) {
	client, err := createClient(address, c.systemCertPool)
	if err != nil {
		return nil, fmt.Errorf("error creating Spice Flight client: %w", err)
	}
	defer client.Close()

	authContext, err := client.AuthenticateBasicToken(ctx, appId, apiKey)
	if err != nil {
		return nil, fmt.Errorf("error authenticating with Spice.xyz: %w", err)
	}

	fd := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(sql),
	}

	var info *flight.FlightInfo
	info, err = client.GetFlightInfo(authContext, fd)
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

	return rdr, err
}

func createClient(address string, systemCertPool *x509.CertPool) (flight.Client, error) {
	grpcDialOpts := []grpc.DialOption{grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(MAX_MESSAGE_SIZE_BYTES),
		grpc.MaxCallSendMsgSize(MAX_MESSAGE_SIZE_BYTES))}

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
