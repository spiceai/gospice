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
	MAX_MESSAGE_SIZE_BYTES = 100 * 1024 * 1024
)

// SpiceClient is a client for Spice.xyz - Data and AI infrastructure for web3
// https://spice.xyz
// For documentation visit https://docs.spice.xyz/sdks/go-sdk
type SpiceClient struct {
	authContext  context.Context
	flightClient flight.Client
}

// NewSpiceClient creates a new SpiceClient
func NewSpiceClient() *SpiceClient {
	return &SpiceClient{}
}

// Init initializes the SpiceClient
func (c *SpiceClient) Init(ctx context.Context, apiKey string) error {
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

	authContext, err := client.AuthenticateBasicToken(ctx, apiKeyParts[0], apiKey)
	if err != nil {
		return fmt.Errorf("Error authenticating with Spice.xyz: %w", err)
	}
	c.authContext = authContext
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
func (c *SpiceClient) Query(query string) (array.RecordReader, error) {
	if c.flightClient == nil || c.authContext == nil {
		return nil, fmt.Errorf("SpiceClient is not initialized")
	}

	fd := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(query),
	}

	var info *flight.FlightInfo
	info, err := c.flightClient.GetFlightInfo(c.authContext, fd)
	if err != nil {
		return nil, err
	}

	stream, err := c.flightClient.DoGet(c.authContext, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, err
	}

	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, err
	}

	return rdr, err
}
