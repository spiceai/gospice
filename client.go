package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	MAX_MESSAGE_SIZE_BYTES   = 100 * 1024 * 1024
	DEFAULT_QUERY_TIMEOUT_MS = 10 * 60 * 1000 // 10 minutes
)

// SpiceClient is a client for Spice.xyz - Data and AI infrastructure for web3
// https://spice.xyz
// For documentation visit https://docs.spice.xyz/sdks/go-sdk
type SpiceClient struct {
	appId        string
	apiKey       string
	flightClient flight.Client
}

// NewSpiceClient creates a new SpiceClient
func NewSpiceClient() *SpiceClient {
	return &SpiceClient{}
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

	// Creating client connected to Spice
	client, err := flight.NewClientWithMiddleware(
		"flight.spiceai.io:443",
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MAX_MESSAGE_SIZE_BYTES),
			grpc.MaxCallSendMsgSize(MAX_MESSAGE_SIZE_BYTES),
		),
	)
	if err != nil {
		return fmt.Errorf("Error creating Spice Flight client: %w", err)
	}

	c.appId = apiKeyParts[0]
	c.apiKey = apiKey
	c.flightClient = client

	return nil
}

// Close closes the SpiceClient and cleans up resources
func (c *SpiceClient) Close() error {
	if c.flightClient != nil {
		return c.flightClient.Close()
	}
	return nil
}

// Query executes a query against Spice.xyz and returns a Apache Arrow RecordReader
// For more information on Apache Arrow RecordReader visit https://godoc.org/github.com/apache/arrow/go/arrow/array#RecordReader
func (c *SpiceClient) Query(ctx context.Context, query string) (array.RecordReader, error) {
	if c.flightClient == nil {
		return nil, fmt.Errorf("SpiceClient is not initialized")
	}

	_, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DEFAULT_QUERY_TIMEOUT_MS)
		defer cancel()
	}

	authContext, err := c.flightClient.AuthenticateBasicToken(ctx, c.appId, c.apiKey)
	if err != nil {
		return nil, fmt.Errorf("Error authenticating with Spice.xyz: %w", err)
	}

	fd := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(query),
	}

	var info *flight.FlightInfo
	info, err = c.flightClient.GetFlightInfo(authContext, fd)
	if err != nil {
		return nil, err
	}

	stream, err := c.flightClient.DoGet(authContext, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, err
	}

	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, err
	}

	return rdr, err
}
