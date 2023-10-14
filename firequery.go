package gospice

import (
	"context"

	"github.com/apache/arrow/go/v13/arrow/array"
)

// FireQuery executes a query against Spice.xyz Firecache and returns a Apache Arrow RecordReader
// For more information on Apache Arrow RecordReader visit https://godoc.org/github.com/apache/arrow/go/arrow/array#RecordReader
func (c *SpiceClient) FireQuery(ctx context.Context, sql string) (array.RecordReader, error) {
	return c.query(ctx, c.firecacheClient, c.appId, c.apiKey, sql)
}
