package gospice

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/bradleyjkemp/cupaloy"
)

const (
	TEST_API_KEY = "323337|b42eceab2e7c4a60a04ad57bebea830d" // spice.xyz/spicehq/gospice-tests
)

// Execute a basic query and check for columns and rows
func TestBasicQuery(t *testing.T) {
	spice := NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(TEST_API_KEY); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	t.Run("Recent Ethereum Blocks", func(t *testing.T) {
		reader, err := spice.Query(context.Background(), "SELECT number, \"timestamp\", hash FROM eth.recent_blocks ORDER BY number LIMIT 10")
		if err != nil {
			panic(fmt.Errorf("error querying: %w", err))
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("number") {
			t.Fatalf("Schema does not have field 'number'")
		}
		if !schema.HasField("timestamp") {
			t.Fatalf("Schema does not have field 'timestamp'")
		}
		if !schema.HasField("hash") {
			t.Fatalf("Schema does not have field 'hash'")
		}

		for reader.Next() {
			record := reader.Record()
			defer record.Release()

			if record.NumRows() != 10 {
				t.Fatalf("Expected 10 rows, got %d", record.NumRows())
			}

			col0 := record.Column(0)
			defer col0.Release()

			blockNumber := col0.(*array.Int64).Value(0)
			if blockNumber <= 16410468 {
				t.Fatalf("Expected block number > 16410468, got %d", blockNumber)
			}

			col1 := record.Column(1)
			defer col1.Release()

			timestamp := col1.(*array.Int64).Value(0)
			fiveMinutesAgo := time.Now().Add(-time.Minute * 5).Unix()
			if timestamp > fiveMinutesAgo {
				t.Fatalf("Expected timestamp > %d, got %d", fiveMinutesAgo, timestamp)
			}

			col2 := record.Column(2)
			defer col2.Release()

			hash := col2.(*array.String).Value(0)
			if len(hash) != 66 {
				t.Fatalf("Expected hash length 66, got %d", len(hash))
			}
		}
	})

	t.Run("Recent Polygon Blocks", func(t *testing.T) {
		reader, err := spice.Query(context.Background(), "SELECT number, \"timestamp\", hash FROM polygon.recent_blocks ORDER BY number LIMIT 10")
		if err != nil {
			panic(fmt.Errorf("error querying: %w", err))
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("number") {
			t.Fatalf("Schema does not have field 'number'")
		}
		if !schema.HasField("timestamp") {
			t.Fatalf("Schema does not have field 'timestamp'")
		}
		if !schema.HasField("hash") {
			t.Fatalf("Schema does not have field 'hash'")
		}

		for reader.Next() {
			record := reader.Record()
			defer record.Release()

			if record.NumRows() != 10 {
				t.Fatalf("Expected 10 rows, got %d", record.NumRows())
			}

			col0 := record.Column(0)
			defer col0.Release()

			blockNumber := col0.(*array.Int64).Value(0)
			if blockNumber <= 38099309 {
				t.Fatalf("Expected block number > 38099309, got %d", blockNumber)
			}

			col1 := record.Column(1)
			defer col1.Release()

			timestamp := col1.(*array.Int64).Value(0)
			fiveMinutesAgo := time.Now().Add(-time.Minute * 5).Unix()
			if timestamp > fiveMinutesAgo {
				t.Fatalf("Expected timestamp > %d, got %d", fiveMinutesAgo, timestamp)
			}

			col2 := record.Column(2)
			defer col2.Release()

			hash := col2.(*array.String).Value(0)
			if len(hash) != 66 {
				t.Fatalf("Expected hash length 66, got %d", len(hash))
			}
		}
	})

	// Test Prices
	t.Run("Test prices latest", func(t *testing.T) {
		quote, err := spice.GetPrices(context.Background(), "eth-usd", nil)
		if err != nil {
			t.Fatalf("error querying: %s", err.Error())
		}

		if quote == nil {
			t.Fatalf("expected quote, got nil")
		}

		if len(quote.Prices) != 10 {
			t.Fatalf("expected 10 prices, got %d %+v", len(quote.Prices), quote.Prices)
		}

		if quote.Pair != "ETH-USD" {
			t.Fatalf("expected ETH-USD, got %s", quote.Pair)
		}

		if quote.Prices[0].Price == 0 {
			t.Fatalf("expected price > 0, got %f", quote.Prices[0].Price)
		}
	})

	// TODO(mitch): uncomment once fix for ordered prices is deployed
	// t.Run("Test prices in specific range", func(t *testing.T) {
	// 	params := &QuoteParams{
	// 		StartTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	// 		EndTime:   time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC),
	// 	}

	// 	quote, err := spice.GetPrices(context.Background(), "eth-usd", params)
	// 	if err != nil {
	// 		t.Fatalf("error querying: %s", err.Error())
	// 	}

	// 	cupaloy.SnapshotT(t, quote)
	// })

	t.Run("Test prices in specific range with specific duration", func(t *testing.T) {
		params := &QuoteParams{
			StartTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			EndTime:     time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC),
			Granularity: "24h",
		}

		quote, err := spice.GetPrices(context.Background(), "eth-usd", params)
		if err != nil {
			t.Fatalf("error querying: %s", err.Error())
		}

		cupaloy.SnapshotT(t, quote)
	})
}
