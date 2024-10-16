package gospice

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Query executes a query against Spice.ai and returns a Apache Arrow RecordReader
// For more information on Apache Arrow RecordReader visit https://godoc.org/github.com/apache/arrow/go/arrow/array#RecordReader
func (c *SpiceClient) Query(ctx context.Context, sql string) (array.RecordReader, error) {
	return c.query(ctx, c.flightClient, c.appId, c.apiKey, sql)
}

// internal wrapper for running queries
func (c *SpiceClient) query(ctx context.Context, client flight.Client, appId string, apiKey string, sql string) (array.RecordReader, error) {
	var rdr array.RecordReader
	err := backoff.Retry(func() error {
		var err error
		rdr, err = queryInternal(ctx, client, appId, apiKey, sql)
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

func queryInternal(ctx context.Context, client flight.Client, appId string, apiKey string, sql string) (array.RecordReader, error) {
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
