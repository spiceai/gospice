package gospice

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	TEST_API_KEY = "323337|b42eceab2e7c4a60a04ad57bebea830d" // spice.ai/spicehq/gospice-tests
)

// Execute a basic query and check for columns and rows
func TestBasicQuery(t *testing.T) {
	spice := NewSpiceClient()
	defer func() { _ = spice.Close() }()

	var ApiKey string
	if v, exists := os.LookupEnv("SPICE_API_KEY"); exists {
		ApiKey = v
	} else {
		ApiKey = TEST_API_KEY
	}

	if err := spice.Init(WithApiKey(ApiKey), WithSpiceCloudAddress()); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	t.Run("Recent Ethereum Blocks", func(t *testing.T) {
		t.Skip()
		reader, err := spice.Query(context.Background(), "SELECT number, \"timestamp\", hash FROM eth.recent_blocks ORDER BY number LIMIT 10")
		if err != nil {
			t.Fatalf("error querying: %v", err)
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
			record := reader.RecordBatch()
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
}

// Requires local spice running. Follow the quickstart https://github.com/spiceai/spiceai.

func TestLocalRuntime(t *testing.T) {
	spice := NewSpiceClient()
	defer func() { _ = spice.Close() }()

	if err := spice.Init(WithHttpAddress("http://localhost:8090")); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()

	// Check if Spice is healthy
	if !spice.IsSpiceHealthy(ctx) {
		t.Fatal("Spice instance is not healthy")
	}

	// Wait for Spice to be ready (with timeout)
	timeout := time.After(120 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	ready := false
	for !ready {
		select {
		case <-timeout:
			t.Fatal("Timed out waiting for Spice to be ready")
		case <-ticker.C:
			if spice.IsSpiceReady(ctx) {
				ready = true
			}
		}
	}

	t.Run("Query Local Dataset", func(t *testing.T) {
		reader, err := spice.Query(ctx, "select * from taxi_trips limit 3;")
		if err != nil {
			t.Fatalf("error querying: %v", err)
		}
		defer reader.Release()

		for reader.Next() {
			record := reader.RecordBatch()
			defer record.Release()
		}
	})
}

// TestSqlWithoutAuth tests the Sql method without authentication against local Spice runtime.
// This verifies that the fix for "no authorization header on the response" works correctly.
func TestSqlWithoutAuth(t *testing.T) {
	spice := NewSpiceClient()
	defer func() { _ = spice.Close() }()

	// Initialize without API key - this should work with local runtime
	if err := spice.Init(WithHttpAddress("http://localhost:8090")); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()

	// Check if Spice is healthy
	if !spice.IsSpiceHealthy(ctx) {
		t.Skip("Local Spice instance is not healthy, skipping test")
	}

	// Wait for Spice to be ready (with timeout)
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	ready := false
	for !ready {
		select {
		case <-timeout:
			t.Skip("Timed out waiting for Spice to be ready, skipping test")
		case <-ticker.C:
			if spice.IsSpiceReady(ctx) {
				ready = true
			}
		}
	}

	t.Run("Sql without auth - simple query", func(t *testing.T) {
		reader, err := spice.Sql(ctx, "SELECT 1 as num")
		if err != nil {
			t.Fatalf("Sql without auth failed: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("num") {
			t.Fatalf("Schema does not have field 'num'")
		}

		rowCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			rowCount += int(record.NumRows())
			record.Release()
		}

		if rowCount != 1 {
			t.Fatalf("Expected 1 row, got %d", rowCount)
		}
	})

	t.Run("Sql without auth - dataset query", func(t *testing.T) {
		reader, err := spice.Sql(ctx, "SELECT trip_distance, fare_amount FROM taxi_trips LIMIT 5")
		if err != nil {
			t.Fatalf("Sql without auth failed: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("trip_distance") {
			t.Fatalf("Schema does not have field 'trip_distance'")
		}
		if !schema.HasField("fare_amount") {
			t.Fatalf("Schema does not have field 'fare_amount'")
		}

		rowCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			rowCount += int(record.NumRows())
			record.Release()
		}

		if rowCount == 0 {
			t.Fatalf("Expected at least 1 row, got 0")
		}
		if rowCount > 5 {
			t.Fatalf("Expected at most 5 rows, got %d", rowCount)
		}
		t.Logf("Sql without auth returned %d rows", rowCount)
	})
}

// TestSqlWithAuth tests the Sql method with authentication against Spice Cloud.
func TestSqlWithAuth(t *testing.T) {
	spice := NewSpiceClient()
	defer func() { _ = spice.Close() }()

	var ApiKey string
	if v, exists := os.LookupEnv("SPICE_API_KEY"); exists {
		ApiKey = v
	} else {
		ApiKey = TEST_API_KEY
	}

	if err := spice.Init(WithApiKey(ApiKey), WithSpiceCloudAddress()); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()

	// Check if Spice Cloud is healthy and ready
	if !spice.IsSpiceHealthy(ctx) {
		t.Skip("Spice Cloud is not healthy, skipping test")
	}
	if !spice.IsSpiceReady(ctx) {
		t.Skip("Spice Cloud is not ready (check API key), skipping test")
	}

	t.Run("Sql with auth - simple query", func(t *testing.T) {
		reader, err := spice.Sql(ctx, "SELECT 1 as num")
		if err != nil {
			t.Fatalf("Sql with auth failed: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("num") {
			t.Fatalf("Schema does not have field 'num'")
		}

		rowCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			rowCount += int(record.NumRows())
			record.Release()
		}

		if rowCount != 1 {
			t.Fatalf("Expected 1 row, got %d", rowCount)
		}
	})

	t.Run("Sql with auth - TPC-H query", func(t *testing.T) {
		reader, err := spice.Sql(ctx, "SELECT c_custkey, c_name FROM tpch.customer ORDER BY c_custkey LIMIT 5")
		if err != nil {
			t.Fatalf("Sql with auth failed: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("c_custkey") {
			t.Fatalf("Schema does not have field 'c_custkey'")
		}
		if !schema.HasField("c_name") {
			t.Fatalf("Schema does not have field 'c_name'")
		}

		rowCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			rowCount += int(record.NumRows())
			record.Release()
		}

		if rowCount == 0 {
			t.Fatalf("Expected at least 1 row, got 0")
		}
		if rowCount > 5 {
			t.Fatalf("Expected at most 5 rows, got %d", rowCount)
		}
		t.Logf("Sql with auth returned %d rows", rowCount)
	})
}
