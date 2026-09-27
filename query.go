package gospice

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Sql executes a SQL query against Spice.ai and returns an Apache Arrow RecordReader
// For more information on Apache Arrow RecordReader visit https://godoc.org/github.com/apache/arrow/go/arrow/array#RecordReader
func (c *SpiceClient) Sql(ctx context.Context, sql string) (array.RecordReader, error) {
	var rdr array.RecordReader
	err := backoff.Retry(func() error {
		err := c.withSession(ctx, func(ctx context.Context) error {
			var err error
			rdr, err = queryInternal(ctx, c.flightClient, sql)
			return err
		})
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

// queryInternal runs sql over client. ctx must already carry the Flight session
// (see withSession).
func queryInternal(ctx context.Context, client flight.Client, sql string) (array.RecordReader, error) {
	if client == nil {
		return nil, fmt.Errorf("flight client is not initialized")
	}
	queryCtx := ctx

	fd := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(sql),
	}

	info, err := client.GetFlightInfo(queryCtx, fd)
	if err != nil {
		return nil, err
	}

	// A well-behaved server always returns at least one endpoint to read results
	// from. Check rather than index blindly, so a server that doesn't surfaces as
	// an error instead of panicking in the caller's process.
	if len(info.Endpoint) == 0 {
		return nil, fmt.Errorf("query returned no Flight endpoint to read results from")
	}

	stream, err := client.DoGet(queryCtx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, err
	}

	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		// Ensure stream is closed if reader creation fails
		if closeErr := stream.CloseSend(); closeErr != nil {
			return nil, fmt.Errorf("error creating record reader: %w (failed to close stream: %v)", err, closeErr)
		}
		return nil, err
	}

	return rdr, nil
}
