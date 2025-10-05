package gospice

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	TEST_API_KEY = "323337|b42eceab2e7c4a60a04ad57bebea830d" // spice.ai/spicehq/gospice-tests
)

// TestCloudBasicQuery tests basic query functionality against Spice Cloud
func TestCloudBasicQuery(t *testing.T) {
	spice := NewSpiceClient()
	defer spice.Close()

	var ApiKey string
	if v, exists := os.LookupEnv("SPICE_API_KEY"); exists {
		ApiKey = v
	} else {
		ApiKey = TEST_API_KEY
	}

	if err := spice.Init(WithApiKey(ApiKey), WithSpiceCloudAddress()); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	// Check if Spice Cloud is healthy and ready
	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		t.Skip("Skipping - Spice Cloud is not healthy")
	}
	if !spice.IsSpiceReady(ctx) {
		t.Skip("Skipping - Spice Cloud is not ready (check API key)")
	}

	t.Run("Cloud - TPC-H Customer Query", func(t *testing.T) {
		reader, err := spice.Query(context.Background(), "SELECT c_custkey, c_name, c_nationkey FROM tpch.customer ORDER BY c_custkey LIMIT 10")
		if err != nil {
			t.Skipf("Skipping - error querying: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("c_custkey") {
			t.Fatalf("Schema does not have field 'c_custkey'")
		}
		if !schema.HasField("c_name") {
			t.Fatalf("Schema does not have field 'c_name'")
		}
		if !schema.HasField("c_nationkey") {
			t.Fatalf("Schema does not have field 'c_nationkey'")
		}

		recordCount := 0
		for reader.Next() {
			record := reader.Record()
			defer record.Release()
			recordCount++

			if recordCount == 1 {
				col0 := record.Column(0)
				defer col0.Release()

				// Handle both Int32 and Int64 types
				var custKey int64
				switch v := col0.(type) {
				case *array.Int64:
					custKey = v.Value(0)
				case *array.Int32:
					custKey = int64(v.Value(0))
				default:
					t.Fatalf("Unexpected type for c_custkey: %T", col0)
				}
				if custKey <= 0 {
					t.Fatalf("Expected customer key > 0, got %d", custKey)
				}

				col1 := record.Column(1)
				defer col1.Release()

				// Handle both String and LargeString types
				var name string
				switch v := col1.(type) {
				case *array.String:
					name = v.Value(0)
				case *array.LargeString:
					name = v.Value(0)
				default:
					t.Fatalf("Unexpected type for c_name: %T", col1)
				}
				if len(name) == 0 {
					t.Fatalf("Expected non-empty customer name")
				}

				col2 := record.Column(2)
				defer col2.Release()

				// Handle both Int32 and Int64 types
				var nationKey int64
				switch v := col2.(type) {
				case *array.Int64:
					nationKey = v.Value(0)
				case *array.Int32:
					nationKey = int64(v.Value(0))
				default:
					t.Fatalf("Unexpected type for c_nationkey: %T", col2)
				}
				if nationKey < 0 || nationKey > 24 {
					t.Fatalf("Expected nation key between 0 and 24, got %d", nationKey)
				}
			}
		}

		if recordCount == 0 {
			t.Fatalf("Expected at least 1 row, got 0")
		}
	})

	t.Run("Cloud - Simple SELECT Query", func(t *testing.T) {
		reader, err := spice.Query(context.Background(), "SELECT 1 as number")
		if err != nil {
			t.Skipf("Skipping - error querying: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("number") {
			t.Fatalf("Schema does not have field 'number'")
		}

		for reader.Next() {
			record := reader.Record()
			defer record.Release()
		}
	})

	t.Run("Cloud - TPC-H Multiple Data Types", func(t *testing.T) {
		reader, err := spice.Sql(context.Background(),
			"SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment FROM tpch.customer ORDER BY c_custkey LIMIT 5")
		if err != nil {
			t.Skipf("Skipping - error querying: %v", err)
		}
		defer reader.Release()

		// Validate schema has all expected fields
		expectedFields := []string{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"}
		schema := reader.Schema()
		for _, field := range expectedFields {
			if !schema.HasField(field) {
				t.Fatalf("Schema does not have field '%s'", field)
			}
		}

		totalRows := 0
		for reader.Next() {
			record := reader.Record()

			// Validate each column type and value
			for i := 0; i < int(record.NumRows()); i++ {
				totalRows++
				// c_custkey (integer)
				custKeyCol := record.Column(0)
				if !custKeyCol.IsNull(i) {
					switch v := custKeyCol.(type) {
					case *array.Int64:
						if v.Value(i) <= 0 {
							t.Errorf("Expected c_custkey > 0, got %d", v.Value(i))
						}
					case *array.Int32:
						if v.Value(i) <= 0 {
							t.Errorf("Expected c_custkey > 0, got %d", v.Value(i))
						}
					}
				}

				// c_name (string)
				nameCol := record.Column(1)
				if !nameCol.IsNull(i) {
					var name string
					switch v := nameCol.(type) {
					case *array.String:
						name = v.Value(i)
					case *array.LargeString:
						name = v.Value(i)
					}
					if len(name) == 0 {
						t.Errorf("Expected non-empty customer name")
					}
				}

				// c_nationkey (integer, 0-24)
				nationKeyCol := record.Column(3)
				if !nationKeyCol.IsNull(i) {
					var nationKey int64
					switch v := nationKeyCol.(type) {
					case *array.Int64:
						nationKey = v.Value(i)
					case *array.Int32:
						nationKey = int64(v.Value(i))
					}
					if nationKey < 0 || nationKey > 24 {
						t.Errorf("Expected nation key 0-24, got %d", nationKey)
					}
				}

				// c_acctbal (float)
				acctBalCol := record.Column(5)
				if !acctBalCol.IsNull(i) {
					switch v := acctBalCol.(type) {
					case *array.Float64:
						// Just check it's a valid number
						if v.Value(i) != v.Value(i) { // NaN check
							t.Errorf("Expected valid c_acctbal, got NaN")
						}
					case *array.Float32:
						if v.Value(i) != v.Value(i) { // NaN check
							t.Errorf("Expected valid c_acctbal, got NaN")
						}
					}
				}
			}

			record.Release()
		}

		if totalRows != 5 {
			t.Fatalf("Expected 5 rows, got %d", totalRows)
		}
	})

	t.Run("Cloud - TPC-H Lineitem Data Types", func(t *testing.T) {
		reader, err := spice.Sql(context.Background(),
			"SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate FROM tpch.lineitem ORDER BY l_orderkey, l_linenumber LIMIT 10")
		if err != nil {
			t.Skipf("Skipping - error querying: %v", err)
		}
		defer reader.Release()

		// Validate schema
		expectedFields := []string{"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate"}
		schema := reader.Schema()
		for _, field := range expectedFields {
			if !schema.HasField(field) {
				t.Fatalf("Schema does not have field '%s'", field)
			}
		}

		totalRows := 0
		for reader.Next() {
			record := reader.Record()

			// Validate numeric fields are positive and within expected ranges
			for i := 0; i < int(record.NumRows()); i++ {
				totalRows++
				// l_quantity (float, should be > 0)
				quantityCol := record.Column(4)
				if !quantityCol.IsNull(i) {
					switch v := quantityCol.(type) {
					case *array.Float64:
						if v.Value(i) <= 0 {
							t.Errorf("Expected l_quantity > 0, got %f", v.Value(i))
						}
					case *array.Float32:
						if v.Value(i) <= 0 {
							t.Errorf("Expected l_quantity > 0, got %f", v.Value(i))
						}
					}
				}

				// l_discount (float, should be 0.0-1.0)
				discountCol := record.Column(6)
				if !discountCol.IsNull(i) {
					switch v := discountCol.(type) {
					case *array.Float64:
						if v.Value(i) < 0 || v.Value(i) > 0.1 {
							t.Errorf("Expected l_discount 0.0-0.1, got %f", v.Value(i))
						}
					case *array.Float32:
						if v.Value(i) < 0 || v.Value(i) > 0.1 {
							t.Errorf("Expected l_discount 0.0-0.1, got %f", v.Value(i))
						}
					}
				}

				// l_returnflag (string, should be single character)
				returnFlagCol := record.Column(8)
				if !returnFlagCol.IsNull(i) {
					var flag string
					switch v := returnFlagCol.(type) {
					case *array.String:
						flag = v.Value(i)
					case *array.LargeString:
						flag = v.Value(i)
					}
					if len(flag) != 1 {
						t.Errorf("Expected l_returnflag to be single character, got '%s'", flag)
					}
				}
			}

			record.Release()
		}

		if totalRows != 10 {
			t.Fatalf("Expected 10 rows, got %d", totalRows)
		}
	})
}

// TestLocalBasicQuery tests basic query functionality with local Spice runtime
func TestLocalBasicQuery(t *testing.T) {
	spice := NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	// Check if local Spice runtime is healthy
	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		t.Skip("Skipping - local Spice runtime is not healthy")
	}

	t.Run("Local - Query Taxi Trips Dataset", func(t *testing.T) {
		// Use deterministic query with ORDER BY and specific filters
		reader, err := spice.Query(context.Background(),
			"SELECT trip_distance, fare_amount, payment_type FROM taxi_trips WHERE trip_distance > 0 ORDER BY trip_distance LIMIT 3")
		if err != nil {
			t.Skipf("Skipping - requires local spice runtime with taxi_trips dataset: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("trip_distance") {
			t.Fatalf("Schema does not have field 'trip_distance'")
		}
		if !schema.HasField("fare_amount") {
			t.Fatalf("Schema does not have field 'fare_amount'")
		}
		if !schema.HasField("payment_type") {
			t.Fatalf("Schema does not have field 'payment_type'")
		}

		recordCount := 0
		for reader.Next() {
			record := reader.Record()
			recordCount++

			// Validate data types and values
			if record.NumCols() != 3 {
				t.Fatalf("Expected 3 columns, got %d", record.NumCols())
			}

			// Validate trip_distance is positive
			col0 := record.Column(0)
			for i := 0; i < int(record.NumRows()); i++ {
				if col0.IsNull(i) {
					continue
				}
				switch v := col0.(type) {
				case *array.Float64:
					if v.Value(i) <= 0 {
						t.Errorf("Expected trip_distance > 0, got %f", v.Value(i))
					}
				case *array.Float32:
					if v.Value(i) <= 0 {
						t.Errorf("Expected trip_distance > 0, got %f", v.Value(i))
					}
				}
			}

			record.Release()
		}

		if recordCount == 0 {
			t.Fatalf("Expected at least 1 record, got 0")
		}
	})

	t.Run("Local - Simple SELECT Query", func(t *testing.T) {
		reader, err := spice.Query(context.Background(), "SELECT 1 as number")
		if err != nil {
			t.Skipf("Skipping - requires local spice runtime: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("number") {
			t.Fatalf("Schema does not have field 'number'")
		}

		for reader.Next() {
			record := reader.Record()
			record.Release()
		}
	})
}
