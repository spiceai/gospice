package gospice

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// TestADBCCloudBasicQuery tests basic ADBC query functionality against Spice Cloud
func TestADBCCloudBasicQuery(t *testing.T) {
	// Uses SPICE_FLIGHT_URL and SPICE_HTTP_URL env vars if set
	// e.g., SPICE_FLIGHT_URL="dev-data.spiceai.io:443" SPICE_HTTP_URL="https://dev-data.spiceai.io" for dev

	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			t.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

	ApiKey, exists := os.LookupEnv("SPICE_API_KEY")
	if !exists || ApiKey == "" {
		t.Skip("SPICE_API_KEY not set, skipping cloud authentication test")
	}

	if err := spice.Init(WithApiKey(ApiKey), WithSpiceCloudAddress()); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	// Check if Spice Cloud is healthy and ready
	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		t.Fatal("Spice Cloud is not healthy")
	}
	if !spice.IsSpiceReady(ctx) {
		t.Fatal("Spice Cloud is not ready (check API key)")
	}

	t.Run("Cloud - Simple ADBC Query", func(t *testing.T) {
		reader, err := spice.SqlWithParams(context.Background(), "SELECT 1 as number")
		if err != nil {
			t.Fatalf("error querying: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("number") {
			t.Fatalf("Schema does not have field 'number'")
		}

		for reader.Next() {
			record := reader.RecordBatch()

			if record.NumRows() == 0 {
				record.Release()
				t.Fatalf("Expected at least 1 row, got %d", record.NumRows())
			}
			record.Release()
		}
	})

	t.Run("Cloud - TPC-H Parameterized Query", func(t *testing.T) {
		reader, err := spice.SqlWithParams(
			context.Background(),
			"SELECT c_custkey, c_name FROM tpch.customer WHERE c_custkey > $1 ORDER BY c_custkey LIMIT 5",
			100,
		)
		if err != nil {
			t.Fatalf("error querying: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("c_custkey") {
			t.Fatalf("Schema does not have field 'c_custkey'")
		}
		if !schema.HasField("c_name") {
			t.Fatalf("Schema does not have field 'c_name'")
		}

		recordCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			recordCount++

			// Validate c_custkey > 100
			custKeyCol := record.Column(0)
			for i := 0; i < int(record.NumRows()); i++ {
				if !custKeyCol.IsNull(i) {
					switch v := custKeyCol.(type) {
					case *array.Int64:
						if v.Value(i) <= 100 {
							t.Errorf("Expected c_custkey > 100, got %d", v.Value(i))
						}
					case *array.Int32:
						if v.Value(i) <= 100 {
							t.Errorf("Expected c_custkey > 100, got %d", v.Value(i))
						}
					}
				}
			}

			// Validate c_name is non-empty
			nameCol := record.Column(1)
			for i := 0; i < int(record.NumRows()); i++ {
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
			}

			record.Release()
		}

		if recordCount > 5 {
			t.Fatalf("Expected at most 5 records, got %d", recordCount)
		}
	})

	t.Run("Cloud - TPC-H Multiple Parameter Types", func(t *testing.T) {
		reader, err := spice.SqlWithParams(
			context.Background(),
			"SELECT c_custkey, c_name, c_acctbal, c_mktsegment FROM tpch.customer WHERE c_custkey > $1 AND c_acctbal > $2 AND c_mktsegment = $3 ORDER BY c_custkey LIMIT 10",
			100,
			5000.0,
			"BUILDING",
		)
		if err != nil {
			t.Fatalf("error querying: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		fields := []string{"c_custkey", "c_name", "c_acctbal", "c_mktsegment"}
		for _, field := range fields {
			if !schema.HasField(field) {
				t.Fatalf("Schema does not have field '%s'", field)
			}
		}

		recordCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			recordCount++

			// Validate all parameter conditions
			for i := 0; i < int(record.NumRows()); i++ {
				// c_custkey > 100
				custKeyCol := record.Column(0)
				if !custKeyCol.IsNull(i) {
					switch v := custKeyCol.(type) {
					case *array.Int64:
						if v.Value(i) <= 100 {
							t.Errorf("Expected c_custkey > 100, got %d", v.Value(i))
						}
					case *array.Int32:
						if v.Value(i) <= 100 {
							t.Errorf("Expected c_custkey > 100, got %d", v.Value(i))
						}
					}
				}

				// c_acctbal > 5000.0
				acctBalCol := record.Column(2)
				if !acctBalCol.IsNull(i) {
					switch v := acctBalCol.(type) {
					case *array.Float64:
						if v.Value(i) <= 5000.0 {
							t.Errorf("Expected c_acctbal > 5000.0, got %f", v.Value(i))
						}
					case *array.Float32:
						if v.Value(i) <= 5000.0 {
							t.Errorf("Expected c_acctbal > 5000.0, got %f", v.Value(i))
						}
					}
				}

				// c_mktsegment = "BUILDING"
				mktSegCol := record.Column(3)
				if !mktSegCol.IsNull(i) {
					var mktSeg string
					switch v := mktSegCol.(type) {
					case *array.String:
						mktSeg = v.Value(i)
					case *array.LargeString:
						mktSeg = v.Value(i)
					}
					if mktSeg != "BUILDING" {
						t.Errorf("Expected c_mktsegment 'BUILDING', got '%s'", mktSeg)
					}
				}
			}

			record.Release()
		}

		if recordCount == 0 {
			t.Log("Note: Query returned 0 rows, which is valid if no data matches the criteria")
		}
	})
}

// TestADBCLocalParameterizedQuery tests parameterized query functionality with local runtime
func TestADBCLocalParameterizedQuery(t *testing.T) {
	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			t.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

	if err := spice.Init(); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	// Check if local Spice runtime is healthy
	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		t.Fatal("local Spice runtime is not healthy")
	}

	t.Run("Local - Parameterized Query with Float", func(t *testing.T) {
		sql := "SELECT trip_distance, fare_amount FROM taxi_trips WHERE trip_distance > $1 ORDER BY trip_distance LIMIT 5"
		reader, err := spice.SqlWithParams(context.Background(), sql, 10.0)
		if err != nil {
			t.Fatalf("error executing parameterized query with float: %v", err)
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

			// Validate trip_distance is > 10.0 (the parameterized value)
			col0 := record.Column(0)
			for i := 0; i < int(record.NumRows()); i++ {
				rowCount++
				if col0.IsNull(i) {
					continue
				}
				switch v := col0.(type) {
				case *array.Float64:
					if v.Value(i) <= 10.0 {
						t.Errorf("Parameterized query failed: Expected trip_distance > 10.0, got %f", v.Value(i))
					}
				case *array.Float32:
					if v.Value(i) <= 10.0 {
						t.Errorf("Parameterized query failed: Expected trip_distance > 10.0, got %f", v.Value(i))
					}
				}
			}

			record.Release()
		}

		if rowCount == 0 {
			t.Fatalf("Expected at least 1 row from parameterized query, got 0")
		}
		if rowCount > 5 {
			t.Fatalf("Expected at most 5 rows (LIMIT 5), got %d", rowCount)
		}
		t.Logf("Parameterized query with float returned %d rows", rowCount)
	})

	t.Run("Local - Parameterized Query with Multiple Parameters", func(t *testing.T) {
		sql := "SELECT trip_distance, fare_amount, payment_type FROM taxi_trips WHERE trip_distance > $1 AND fare_amount > $2 ORDER BY trip_distance, fare_amount LIMIT 3"
		reader, err := spice.SqlWithParams(context.Background(), sql, 5.0, 20.0)
		if err != nil {
			t.Fatalf("error executing parameterized query with multiple params: %v", err)
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

			// Validate both parameters are correctly applied
			tripDistCol := record.Column(0)
			fareCol := record.Column(1)

			for i := 0; i < int(record.NumRows()); i++ {
				rowCount++

				// Validate trip_distance > 5.0 ($1 parameter)
				if !tripDistCol.IsNull(i) {
					switch v := tripDistCol.(type) {
					case *array.Float64:
						if v.Value(i) <= 5.0 {
							t.Errorf("Parameterized query failed ($1): Expected trip_distance > 5.0, got %f", v.Value(i))
						}
					case *array.Float32:
						if v.Value(i) <= 5.0 {
							t.Errorf("Parameterized query failed ($1): Expected trip_distance > 5.0, got %f", v.Value(i))
						}
					}
				}

				// Validate fare_amount > 20.0 ($2 parameter)
				if !fareCol.IsNull(i) {
					switch v := fareCol.(type) {
					case *array.Float64:
						if v.Value(i) <= 20.0 {
							t.Errorf("Parameterized query failed ($2): Expected fare_amount > 20.0, got %f", v.Value(i))
						}
					case *array.Float32:
						if v.Value(i) <= 20.0 {
							t.Errorf("Parameterized query failed ($2): Expected fare_amount > 20.0, got %f", v.Value(i))
						}
					}
				}
			}

			record.Release()
		}

		if rowCount == 0 {
			t.Fatalf("Expected at least 1 row from parameterized query, got 0")
		}
		if rowCount > 3 {
			t.Fatalf("Expected at most 3 rows (LIMIT 3), got %d", rowCount)
		}
		t.Logf("Parameterized query with multiple params returned %d rows", rowCount)
	})

	t.Run("Local - Parameterized Query with String", func(t *testing.T) {
		// Use store_and_fwd_flag which is the string column in taxi_trips (large_utf8)
		// "N" means not a store-and-forward trip, "Y" means it was
		sql := "SELECT trip_distance, fare_amount, store_and_fwd_flag FROM taxi_trips WHERE store_and_fwd_flag = $1 ORDER BY trip_distance LIMIT 5"
		reader, err := spice.SqlWithParams(context.Background(), sql, "N")
		if err != nil {
			t.Fatalf("error executing parameterized query with string: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("store_and_fwd_flag") {
			t.Fatalf("Schema does not have field 'store_and_fwd_flag'")
		}

		rowCount := 0
		for reader.Next() {
			record := reader.RecordBatch()

			// Validate store_and_fwd_flag matches the string parameter exactly
			flagCol := record.Column(2)
			for i := 0; i < int(record.NumRows()); i++ {
				rowCount++
				if !flagCol.IsNull(i) {
					var flag string
					switch v := flagCol.(type) {
					case *array.String:
						flag = v.Value(i)
					case *array.LargeString:
						flag = v.Value(i)
					}
					if flag != "N" {
						t.Errorf("Parameterized query failed: Expected store_and_fwd_flag 'N', got '%s'", flag)
					}
				}
			}

			record.Release()
		}

		if rowCount == 0 {
			t.Fatalf("Expected at least 1 row from parameterized query, got 0")
		}
		if rowCount > 5 {
			t.Fatalf("Expected at most 5 rows (LIMIT 5), got %d", rowCount)
		}
		t.Logf("Parameterized query with string returned %d rows", rowCount)
	})

	t.Run("Local - Parameterized Query with Integer", func(t *testing.T) {
		// Use payment_type which is an integer column (int64)
		// payment_type: 1=Credit Card, 2=Cash, 3=No Charge, 4=Dispute, 0=Unknown
		sql := "SELECT trip_distance, fare_amount, payment_type FROM taxi_trips WHERE payment_type = $1 ORDER BY trip_distance LIMIT 5"
		reader, err := spice.SqlWithParams(context.Background(), sql, int64(1))
		if err != nil {
			t.Fatalf("error executing parameterized query with integer: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("payment_type") {
			t.Fatalf("Schema does not have field 'payment_type'")
		}

		rowCount := 0
		for reader.Next() {
			record := reader.RecordBatch()

			// Validate payment_type matches the integer parameter exactly
			paymentTypeCol := record.Column(2)
			for i := 0; i < int(record.NumRows()); i++ {
				rowCount++
				if !paymentTypeCol.IsNull(i) {
					switch v := paymentTypeCol.(type) {
					case *array.Int64:
						if v.Value(i) != 1 {
							t.Errorf("Parameterized query failed: Expected payment_type 1, got %d", v.Value(i))
						}
					case *array.Int32:
						if v.Value(i) != 1 {
							t.Errorf("Parameterized query failed: Expected payment_type 1, got %d", v.Value(i))
						}
					}
				}
			}

			record.Release()
		}

		if rowCount == 0 {
			t.Fatalf("Expected at least 1 row from parameterized query, got 0")
		}
		if rowCount > 5 {
			t.Fatalf("Expected at most 5 rows (LIMIT 5), got %d", rowCount)
		}
		t.Logf("Parameterized query with integer returned %d rows", rowCount)
	})
}

// TestADBCLocalBasicQuery tests basic ADBC query functionality with local Spice runtime
func TestADBCLocalBasicQuery(t *testing.T) {
	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			t.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

	if err := spice.Init(); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	// Check if local Spice runtime is healthy
	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		t.Fatal("local Spice runtime is not healthy")
	}

	t.Run("Local - Query Dataset with ADBC", func(t *testing.T) {
		reader, err := spice.SqlWithParams(context.Background(),
			"SELECT trip_distance, fare_amount FROM taxi_trips WHERE trip_distance > 0 ORDER BY trip_distance LIMIT 3")
		if err != nil {
			t.Fatalf("error executing ADBC query: %v", err)
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

			// Validate trip_distance is positive (query condition)
			col0 := record.Column(0)
			for i := 0; i < int(record.NumRows()); i++ {
				rowCount++
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

		if rowCount == 0 {
			t.Fatalf("Expected at least 1 row, got 0")
		}
		t.Logf("ADBC query returned %d rows", rowCount)
	})

	t.Run("Local - Simple SELECT 1", func(t *testing.T) {
		reader, err := spice.SqlWithParams(context.Background(), "SELECT 1 as num")
		if err != nil {
			t.Fatalf("error executing ADBC query: %v", err)
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
			t.Fatalf("Expected exactly 1 row for SELECT 1, got %d", rowCount)
		}
	})
}

// TestParameterTypes tests different parameter types
func TestParameterTypes(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"int32", int32(42)},
		{"int64", int64(9223372036854775807)},
		{"float32", float32(3.14)},
		{"float64", float64(2.718281828)},
		{"string", "hello world"},
		{"bool", true},
		{"uint32", uint32(123)},
		{"uint64", uint64(456)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataType, err := inferArrowType(tt.value)
			if err != nil {
				t.Fatalf("error inferring type for %v: %v", tt.value, err)
			}
			if dataType == nil {
				t.Fatalf("got nil data type for %v", tt.value)
			}
		})
	}
}
