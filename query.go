package gospice

import (
	"context"

	"github.com/apache/arrow/go/v16/arrow/array"
)

// Query executes a query against Spice.ai and returns a Apache Arrow RecordReader
// For more information on Apache Arrow RecordReader visit https://godoc.org/github.com/apache/arrow/go/arrow/array#RecordReader
func (c *SpiceClient) Query(ctx context.Context, sql string) (array.RecordReader, error) {
	return c.query(ctx, c.flightClient, c.appId, c.apiKey, sql)
}
