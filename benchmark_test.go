package gospice

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// BenchmarkCloudQuery benchmarks basic query performance against Spice Cloud
func BenchmarkCloudQuery(b *testing.B) {
	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			b.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

	var ApiKey string
	if v, exists := os.LookupEnv("SPICE_API_KEY"); exists {
		ApiKey = v
	} else {
		ApiKey = TEST_API_KEY
	}

	if err := spice.Init(WithApiKey(ApiKey), WithSpiceCloudAddress()); err != nil {
		b.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		b.Skip("Skipping - Spice Cloud is not healthy")
	}
	if !spice.IsSpiceReady(ctx) {
		b.Skip("Skipping - Spice Cloud is not ready")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, err := spice.Sql(ctx, "SELECT c_custkey, c_name FROM tpch.customer ORDER BY c_custkey LIMIT 100")
		if err != nil {
			b.Fatalf("error querying: %v", err)
		}

		for reader.Next() {
			record := reader.RecordBatch()
			record.Release()
		}
		reader.Release()
	}
}

// BenchmarkCloudQueryWithParams benchmarks parameterized query performance
func BenchmarkCloudQueryWithParams(b *testing.B) {
	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			b.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

	var ApiKey string
	if v, exists := os.LookupEnv("SPICE_API_KEY"); exists {
		ApiKey = v
	} else {
		ApiKey = TEST_API_KEY
	}

	if err := spice.Init(WithApiKey(ApiKey), WithSpiceCloudAddress()); err != nil {
		b.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		b.Skip("Skipping - Spice Cloud is not healthy")
	}
	if !spice.IsSpiceReady(ctx) {
		b.Skip("Skipping - Spice Cloud is not ready")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, err := spice.SqlWithParams(ctx,
			"SELECT c_custkey, c_name FROM tpch.customer WHERE c_custkey > $1 ORDER BY c_custkey LIMIT 100",
			100)
		if err != nil {
			b.Fatalf("error querying: %v", err)
		}

		for reader.Next() {
			record := reader.RecordBatch()
			record.Release()
		}
		reader.Release()
	}
}

// BenchmarkLocalQuery benchmarks query performance against local Spice runtime
func BenchmarkLocalQuery(b *testing.B) {
	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			b.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

	if err := spice.Init(); err != nil {
		b.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		b.Skip("Skipping - local Spice runtime is not healthy")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, err := spice.Sql(ctx, "SELECT trip_distance, fare_amount FROM taxi_trips WHERE trip_distance > 0 ORDER BY trip_distance LIMIT 100")
		if err != nil {
			b.Skipf("Skipping - requires local spice runtime with taxi_trips dataset: %v", err)
		}

		for reader.Next() {
			record := reader.RecordBatch()
			record.Release()
		}
		reader.Release()
	}
}

// BenchmarkLocalQueryWithParams benchmarks parameterized query performance locally
func BenchmarkLocalQueryWithParams(b *testing.B) {
	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			b.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

	if err := spice.Init(); err != nil {
		b.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		b.Skip("Skipping - local Spice runtime is not healthy")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, err := spice.SqlWithParams(ctx,
			"SELECT trip_distance, fare_amount FROM taxi_trips WHERE trip_distance > $1 AND fare_amount > $2 ORDER BY trip_distance LIMIT 100",
			5.0, 20.0)
		if err != nil {
			b.Skipf("Skipping - requires local spice runtime with taxi_trips dataset: %v", err)
		}

		for reader.Next() {
			record := reader.RecordBatch()
			record.Release()
		}
		reader.Release()
	}
}

// BenchmarkParameterBinding benchmarks parameter binding overhead
func BenchmarkParameterBinding(b *testing.B) {
	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			b.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

	if err := spice.Init(); err != nil {
		b.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		b.Skip("Skipping - local Spice runtime is not healthy")
	}

	testCases := []struct {
		name   string
		params []interface{}
	}{
		{"1 param", []interface{}{5.0}},
		{"2 params", []interface{}{5.0, 20.0}},
		{"3 params", []interface{}{5.0, 20.0, "Credit Card"}},
		{"5 params", []interface{}{5.0, 20.0, "Credit Card", 10.0, 30.0}},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Build SQL with appropriate number of parameters
			sql := "SELECT 1 WHERE 1=1"
			for i := 1; i <= len(tc.params); i++ {
				sql += " AND $" + string(rune('0'+i)) + " IS NOT NULL"
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader, err := spice.SqlWithParams(ctx, sql, tc.params...)
				if err != nil {
					b.Skipf("Skipping: %v", err)
				}

				for reader.Next() {
					record := reader.RecordBatch()
					record.Release()
				}
				reader.Release()
			}
		})
	}
}

// BenchmarkClientInitialization benchmarks client initialization overhead
func BenchmarkClientInitialization(b *testing.B) {
	b.Run("Local", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			spice := NewSpiceClient()
			if err := spice.Init(); err != nil {
				b.Fatalf("error initializing SpiceClient: %v", err)
			}
			if err := spice.Close(); err != nil {
				b.Logf("warning: failed to close SpiceClient: %v", err)
			}
		}
	})

	b.Run("Cloud", func(b *testing.B) {
		var ApiKey string
		if v, exists := os.LookupEnv("SPICE_API_KEY"); exists {
			ApiKey = v
		} else {
			ApiKey = TEST_API_KEY
		}

		for i := 0; i < b.N; i++ {
			spice := NewSpiceClient()
			if err := spice.Init(WithApiKey(ApiKey), WithSpiceCloudAddress()); err != nil {
				b.Fatalf("error initializing SpiceClient: %v", err)
			}
			if err := spice.Close(); err != nil {
				b.Logf("warning: failed to close SpiceClient: %v", err)
			}
		}
	})
}

// BenchmarkHealthChecks benchmarks health check performance
func BenchmarkHealthChecks(b *testing.B) {
	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			b.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

	if err := spice.Init(); err != nil {
		b.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()

	b.Run("IsSpiceHealthy", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = spice.IsSpiceHealthy(ctx)
		}
	})

	b.Run("IsSpiceReady", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = spice.IsSpiceReady(ctx)
		}
	})
}

// BenchmarkRecordProcessing benchmarks different record processing patterns
func BenchmarkRecordProcessing(b *testing.B) {
	spice := NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			b.Logf("warning: failed to close SpiceClient: %v", err)
		}
	}()

	if err := spice.Init(); err != nil {
		b.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		b.Skip("Skipping - local Spice runtime is not healthy")
	}

	b.Run("Release immediately", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader, err := spice.Sql(ctx, "SELECT trip_distance FROM taxi_trips WHERE trip_distance > 0 ORDER BY trip_distance LIMIT 100")
			if err != nil {
				b.Skipf("Skipping: %v", err)
			}

			for reader.Next() {
				record := reader.RecordBatch()
				record.Release()
			}
			reader.Release()
		}
	})

	b.Run("Process values", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader, err := spice.Sql(ctx, "SELECT trip_distance FROM taxi_trips WHERE trip_distance > 0 ORDER BY trip_distance LIMIT 100")
			if err != nil {
				b.Skipf("Skipping: %v", err)
			}

			var sum float64
			for reader.Next() {
				record := reader.RecordBatch()
				col := record.Column(0)
				for j := 0; j < int(record.NumRows()); j++ {
					if !col.IsNull(j) {
						switch v := col.(type) {
						case *array.Float64:
							sum += v.Value(j)
						case *array.Float32:
							sum += float64(v.Value(j))
						}
					}
				}
				record.Release()
			}
			reader.Release()
			_ = sum // prevent optimization
		}
	})
}
