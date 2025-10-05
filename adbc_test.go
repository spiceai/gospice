package gospice

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// TestADBCCloudBasicQuery tests basic ADBC query functionality against Spice Cloud
func TestADBCCloudBasicQuery(t *testing.T) {
	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			t.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

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

	t.Run("Cloud - Simple ADBC Query", func(t *testing.T) {
		reader, err := spice.QueryWithParams(context.Background(), "SELECT 1 as number")
		if err != nil {
			t.Skipf("Skipping - error querying: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("number") {
			t.Fatalf("Schema does not have field 'number'")
		}

		for reader.Next() {
			record := reader.RecordBatch()
			defer record.Release()

			if record.NumRows() == 0 {
				t.Fatalf("Expected at least 1 row, got %d", record.NumRows())
			}
		}
	})

	t.Run("Cloud - TPC-H Parameterized Query", func(t *testing.T) {
		reader, err := spice.SqlWithParams(
			context.Background(),
			"SELECT c_custkey, c_name FROM tpch.customer WHERE c_custkey > $1 ORDER BY c_custkey LIMIT 5",
			100,
		)
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
			t.Skipf("Skipping - error querying: %v", err)
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
		t.Skip("Skipping - local Spice runtime is not healthy")
	}

	t.Run("Local - Parameterized Query with Float", func(t *testing.T) {
		sql := "SELECT trip_distance, fare_amount FROM taxi_trips WHERE trip_distance > $1 ORDER BY trip_distance LIMIT 5"
		reader, err := spice.QueryWithParams(context.Background(), sql, 10.0)
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

		recordCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			recordCount++

			// Validate trip_distance is > 10.0
			col0 := record.Column(0)
			for i := 0; i < int(record.NumRows()); i++ {
				if col0.IsNull(i) {
					continue
				}
				switch v := col0.(type) {
				case *array.Float64:
					if v.Value(i) <= 10.0 {
						t.Errorf("Expected trip_distance > 10.0, got %f", v.Value(i))
					}
				case *array.Float32:
					if v.Value(i) <= 10.0 {
						t.Errorf("Expected trip_distance > 10.0, got %f", v.Value(i))
					}
				}
			}

			record.Release()
		}

		if recordCount > 5 {
			t.Fatalf("Expected at most 5 records, got %d", recordCount)
		}
	})

	t.Run("Local - Parameterized Query with Multiple Parameters", func(t *testing.T) {
		sql := "SELECT trip_distance, fare_amount, payment_type FROM taxi_trips WHERE trip_distance > $1 AND fare_amount > $2 ORDER BY trip_distance, fare_amount LIMIT 3"
		reader, err := spice.QueryWithParams(context.Background(), sql, 5.0, 20.0)
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

		recordCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			recordCount++

			// Validate both parameters
			tripDistCol := record.Column(0)
			fareCol := record.Column(1)

			for i := 0; i < int(record.NumRows()); i++ {
				// Validate trip_distance > 5.0
				if !tripDistCol.IsNull(i) {
					switch v := tripDistCol.(type) {
					case *array.Float64:
						if v.Value(i) <= 5.0 {
							t.Errorf("Expected trip_distance > 5.0, got %f", v.Value(i))
						}
					case *array.Float32:
						if v.Value(i) <= 5.0 {
							t.Errorf("Expected trip_distance > 5.0, got %f", v.Value(i))
						}
					}
				}

				// Validate fare_amount > 20.0
				if !fareCol.IsNull(i) {
					switch v := fareCol.(type) {
					case *array.Float64:
						if v.Value(i) <= 20.0 {
							t.Errorf("Expected fare_amount > 20.0, got %f", v.Value(i))
						}
					case *array.Float32:
						if v.Value(i) <= 20.0 {
							t.Errorf("Expected fare_amount > 20.0, got %f", v.Value(i))
						}
					}
				}
			}

			record.Release()
		}

		if recordCount > 3 {
			t.Fatalf("Expected at most 3 records, got %d", recordCount)
		}
	})

	t.Run("Local - Parameterized Query with String", func(t *testing.T) {
		sql := "SELECT trip_distance, fare_amount, payment_type FROM taxi_trips WHERE payment_type = $1 ORDER BY trip_distance LIMIT 5"
		reader, err := spice.QueryWithParams(context.Background(), sql, "Credit Card")
		if err != nil {
			t.Skipf("Skipping - requires local spice runtime with taxi_trips dataset: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("payment_type") {
			t.Fatalf("Schema does not have field 'payment_type'")
		}

		recordCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			recordCount++

			// Validate payment_type matches the parameter
			paymentTypeCol := record.Column(2)
			for i := 0; i < int(record.NumRows()); i++ {
				if !paymentTypeCol.IsNull(i) {
					if v, ok := paymentTypeCol.(*array.String); ok {
						if v.Value(i) != "Credit Card" {
							t.Errorf("Expected payment_type 'Credit Card', got '%s'", v.Value(i))
						}
					}
				}
			}

			record.Release()
		}

		if recordCount > 5 {
			t.Fatalf("Expected at most 5 records, got %d", recordCount)
		}
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
		t.Skip("Skipping - local Spice runtime is not healthy")
	}

	t.Run("Local - Query Dataset with ADBC", func(t *testing.T) {
		reader, err := spice.QueryWithParams(context.Background(),
			"SELECT trip_distance, fare_amount FROM taxi_trips WHERE trip_distance > 0 ORDER BY trip_distance LIMIT 3")
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

		recordCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			recordCount++

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

	t.Run("Local - Simple SELECT 1", func(t *testing.T) {
		reader, err := spice.QueryWithParams(context.Background(), "SELECT 1 as num")
		if err != nil {
			t.Skipf("Skipping - requires local spice runtime: %v", err)
		}
		defer reader.Release()

		schema := reader.Schema()
		if !schema.HasField("num") {
			t.Fatalf("Schema does not have field 'num'")
		}

		for reader.Next() {
			record := reader.RecordBatch()
			record.Release()
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
