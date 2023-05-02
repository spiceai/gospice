package gospice

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/flight"
)

// Query executes a query against Spice.xyz and returns a Apache Arrow RecordReader
// For more information on Apache Arrow RecordReader visit https://godoc.org/github.com/apache/arrow/go/arrow/array#RecordReader
func (c *SpiceClient) Query(ctx context.Context, query string) (array.RecordReader, error) {
	if c.flightClient == nil {
		return nil, fmt.Errorf("SpiceClient is not initialized")
	}

	authContext, err := c.flightClient.AuthenticateBasicToken(ctx, c.appId, c.apiKey)
	if err != nil {
		return nil, fmt.Errorf("error authenticating with Spice.xyz: %w", err)
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
