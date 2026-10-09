package gospice

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestInferArrowType verifies that all supported types are correctly inferred to their Arrow equivalents
func TestInferArrowType(t *testing.T) {
	tests := []struct {
		name         string
		value        interface{}
		expectedType arrow.DataType
	}{
		// Integer types
		{"int8", int8(1), arrow.PrimitiveTypes.Int8},
		{"int16", int16(1), arrow.PrimitiveTypes.Int16},
		{"int32", int32(1), arrow.PrimitiveTypes.Int32},
		{"int64", int64(1), arrow.PrimitiveTypes.Int64},
		{"int", int(1), arrow.PrimitiveTypes.Int64}, // Platform-dependent, maps to int64
		{"uint8", uint8(1), arrow.PrimitiveTypes.Uint8},
		{"uint16", uint16(1), arrow.PrimitiveTypes.Uint16},
		{"uint32", uint32(1), arrow.PrimitiveTypes.Uint32},
		{"uint64", uint64(1), arrow.PrimitiveTypes.Uint64},
		{"uint", uint(1), arrow.PrimitiveTypes.Uint64}, // Platform-dependent, maps to uint64

		// Float types
		{"float32", float32(1.0), arrow.PrimitiveTypes.Float32},
		{"float64", float64(1.0), arrow.PrimitiveTypes.Float64},

		// String and binary types
		{"string", "test", arrow.BinaryTypes.String},
		{"bool", true, arrow.FixedWidthTypes.Boolean},
		{"[]byte", []byte("test"), arrow.BinaryTypes.Binary},

		// Temporal types
		{"date32", arrow.Date32(18628), arrow.PrimitiveTypes.Date32},                         // Days since epoch
		{"date64", arrow.Date64(1609459200000), arrow.PrimitiveTypes.Date64},                 // Milliseconds since epoch
		{"time32", arrow.Time32(43200000), arrow.FixedWidthTypes.Time32ms},                   // Milliseconds since midnight
		{"time64", arrow.Time64(43200000000), arrow.FixedWidthTypes.Time64us},                // Microseconds since midnight
		{"timestamp", arrow.Timestamp(1609459200000000), arrow.FixedWidthTypes.Timestamp_us}, // Microseconds since epoch
		{"duration", arrow.Duration(1000000), arrow.FixedWidthTypes.Duration_us},             // Duration in microseconds
		{"time.Time", time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC), arrow.FixedWidthTypes.Timestamp_ns},
		{"time.Duration", 30 * time.Minute, arrow.FixedWidthTypes.Duration_ns},

		// Null type
		{"nil", nil, arrow.Null},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt, err := inferArrowType(tt.value)
			if err != nil {
				t.Fatalf("inferArrowType(%v) failed: %v", tt.value, err)
			}
			if !arrow.TypeEqual(dt, tt.expectedType) {
				t.Errorf("inferArrowType(%v) = %v, want %v", tt.value, dt, tt.expectedType)
			}
		})
	}
}

// TestAppendValueToBuilder verifies that all supported types can be appended to their respective builders
func TestAppendValueToBuilder(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("int8", func(t *testing.T) {
		builder := array.NewInt8Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, int8(42)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Int8).Value(0) != int8(42) {
			t.Errorf("expected 42, got %v", arr.(*array.Int8).Value(0))
		}
	})

	t.Run("int16", func(t *testing.T) {
		builder := array.NewInt16Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, int16(1000)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Int16).Value(0) != int16(1000) {
			t.Errorf("expected 1000, got %v", arr.(*array.Int16).Value(0))
		}
	})

	t.Run("int32", func(t *testing.T) {
		builder := array.NewInt32Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, int32(100000)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Int32).Value(0) != int32(100000) {
			t.Errorf("expected 100000, got %v", arr.(*array.Int32).Value(0))
		}
	})

	t.Run("int64", func(t *testing.T) {
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, int64(1000000000)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Int64).Value(0) != int64(1000000000) {
			t.Errorf("expected 1000000000, got %v", arr.(*array.Int64).Value(0))
		}
	})

	t.Run("int (platform-dependent)", func(t *testing.T) {
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, int(999)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Int64).Value(0) != int64(999) {
			t.Errorf("expected 999, got %v", arr.(*array.Int64).Value(0))
		}
	})

	t.Run("uint8", func(t *testing.T) {
		builder := array.NewUint8Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, uint8(255)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Uint8).Value(0) != uint8(255) {
			t.Errorf("expected 255, got %v", arr.(*array.Uint8).Value(0))
		}
	})

	t.Run("uint16", func(t *testing.T) {
		builder := array.NewUint16Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, uint16(65535)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Uint16).Value(0) != uint16(65535) {
			t.Errorf("expected 65535, got %v", arr.(*array.Uint16).Value(0))
		}
	})

	t.Run("uint32", func(t *testing.T) {
		builder := array.NewUint32Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, uint32(4294967295)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Uint32).Value(0) != uint32(4294967295) {
			t.Errorf("expected 4294967295, got %v", arr.(*array.Uint32).Value(0))
		}
	})

	t.Run("uint64", func(t *testing.T) {
		builder := array.NewUint64Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, uint64(18446744073709551615)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Uint64).Value(0) != uint64(18446744073709551615) {
			t.Errorf("expected 18446744073709551615, got %v", arr.(*array.Uint64).Value(0))
		}
	})

	t.Run("uint (platform-dependent)", func(t *testing.T) {
		builder := array.NewUint64Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, uint(888)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Uint64).Value(0) != uint64(888) {
			t.Errorf("expected 888, got %v", arr.(*array.Uint64).Value(0))
		}
	})

	t.Run("float32", func(t *testing.T) {
		builder := array.NewFloat32Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, float32(3.14)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Float32).Value(0) != float32(3.14) {
			t.Errorf("expected 3.14, got %v", arr.(*array.Float32).Value(0))
		}
	})

	t.Run("float64", func(t *testing.T) {
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, float64(3.141592)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Float64).Value(0) != float64(3.141592) {
			t.Errorf("expected 3.141592, got %v", arr.(*array.Float64).Value(0))
		}
	})

	t.Run("string", func(t *testing.T) {
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, "hello"); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.String).Value(0) != "hello" {
			t.Errorf("expected 'hello', got %v", arr.(*array.String).Value(0))
		}
	})

	t.Run("bool", func(t *testing.T) {
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, true); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Boolean).Value(0) != true {
			t.Errorf("expected true, got %v", arr.(*array.Boolean).Value(0))
		}
	})

	t.Run("[]byte", func(t *testing.T) {
		builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer builder.Release()
		if err := appendValueToBuilder(builder, []byte("world")); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if string(arr.(*array.Binary).Value(0)) != "world" {
			t.Errorf("expected 'world', got %v", string(arr.(*array.Binary).Value(0)))
		}
	})

	// Temporal types
	t.Run("date32", func(t *testing.T) {
		builder := array.NewDate32Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, arrow.Date32(18628)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Date32).Value(0) != arrow.Date32(18628) {
			t.Errorf("expected 18628, got %v", arr.(*array.Date32).Value(0))
		}
	})

	t.Run("date64", func(t *testing.T) {
		builder := array.NewDate64Builder(mem)
		defer builder.Release()
		if err := appendValueToBuilder(builder, arrow.Date64(1609459200000)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Date64).Value(0) != arrow.Date64(1609459200000) {
			t.Errorf("expected 1609459200000, got %v", arr.(*array.Date64).Value(0))
		}
	})

	t.Run("time32", func(t *testing.T) {
		builder := array.NewTime32Builder(mem, arrow.FixedWidthTypes.Time32ms.(*arrow.Time32Type))
		defer builder.Release()
		if err := appendValueToBuilder(builder, arrow.Time32(43200000)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Time32).Value(0) != arrow.Time32(43200000) {
			t.Errorf("expected 43200000, got %v", arr.(*array.Time32).Value(0))
		}
	})

	t.Run("time64", func(t *testing.T) {
		builder := array.NewTime64Builder(mem, arrow.FixedWidthTypes.Time64us.(*arrow.Time64Type))
		defer builder.Release()
		if err := appendValueToBuilder(builder, arrow.Time64(43200000000)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Time64).Value(0) != arrow.Time64(43200000000) {
			t.Errorf("expected 43200000000, got %v", arr.(*array.Time64).Value(0))
		}
	})

	t.Run("timestamp", func(t *testing.T) {
		builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
		defer builder.Release()
		if err := appendValueToBuilder(builder, arrow.Timestamp(1609459200000000)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Timestamp).Value(0) != arrow.Timestamp(1609459200000000) {
			t.Errorf("expected 1609459200000000, got %v", arr.(*array.Timestamp).Value(0))
		}
	})

	t.Run("duration", func(t *testing.T) {
		builder := array.NewDurationBuilder(mem, arrow.FixedWidthTypes.Duration_us.(*arrow.DurationType))
		defer builder.Release()
		if err := appendValueToBuilder(builder, arrow.Duration(1000000)); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if arr.(*array.Duration).Value(0) != arrow.Duration(1000000) {
			t.Errorf("expected 1000000, got %v", arr.(*array.Duration).Value(0))
		}
	})

	// 2024-01-30T19:00:00.123456789-05:00 is 2024-01-31T00:00:00.123456789Z.
	localTime := time.Date(2024, 1, 30, 19, 0, 0, 123456789, time.FixedZone("UTC-5", -5*3600))

	t.Run("time.Time nanoseconds", func(t *testing.T) {
		builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
		defer builder.Release()
		if err := appendValueToBuilder(builder, localTime); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if got := arr.(*array.Timestamp).Value(0); got != arrow.Timestamp(1706659200123456789) {
			t.Errorf("expected 1706659200123456789, got %v", got)
		}
	})

	t.Run("time.Time microseconds", func(t *testing.T) {
		builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
		defer builder.Release()
		if err := appendValueToBuilder(builder, localTime); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if got := arr.(*array.Timestamp).Value(0); got != arrow.Timestamp(1706659200123456) {
			t.Errorf("expected 1706659200123456, got %v", got)
		}
	})

	t.Run("time.Time outside the nanosecond range", func(t *testing.T) {
		for _, v := range []time.Time{
			{},
			time.Date(1677, 9, 21, 0, 12, 43, 0, time.UTC),
			time.Date(2262, 4, 11, 23, 47, 17, 0, time.UTC),
			time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
		} {
			builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
			if err := appendValueToBuilder(builder, v); err == nil {
				t.Errorf("expected an error for %s, got none", v)
			}
			builder.Release()
		}
	})

	t.Run("time.Duration nanoseconds", func(t *testing.T) {
		builder := array.NewDurationBuilder(mem, arrow.FixedWidthTypes.Duration_ns.(*arrow.DurationType))
		defer builder.Release()
		if err := appendValueToBuilder(builder, 90*time.Minute+1500*time.Nanosecond); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if got := arr.(*array.Duration).Value(0); got != arrow.Duration(5400000001500) {
			t.Errorf("expected 5400000001500, got %v", got)
		}
	})

	t.Run("time.Duration microseconds", func(t *testing.T) {
		builder := array.NewDurationBuilder(mem, arrow.FixedWidthTypes.Duration_us.(*arrow.DurationType))
		defer builder.Release()
		if err := appendValueToBuilder(builder, 90*time.Minute+1500*time.Nanosecond); err != nil {
			t.Fatalf("appendValueToBuilder failed: %v", err)
		}
		arr := builder.NewArray()
		defer arr.Release()
		if got := arr.(*array.Duration).Value(0); got != arrow.Duration(5400000001) {
			t.Errorf("expected 5400000001, got %v", got)
		}
	})
}

// TestComprehensiveArrowTypes tests the full pipeline: infer type -> create builder -> append value -> validate
func TestComprehensiveArrowTypes(t *testing.T) {
	mem := memory.NewGoAllocator()

	testValues := []interface{}{
		int8(-128),
		int16(-32768),
		int32(-2147483648),
		int64(-9223372036854775808),
		uint8(255),
		uint16(65535),
		uint32(4294967295),
		uint64(18446744073709551615),
		float32(3.14159),
		float64(2.718281828),
		"comprehensive test",
		true,
		[]byte("binary data"),
		time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC),
		30 * time.Minute,
	}

	for i, val := range testValues {
		t.Run(string(rune(i+'0')), func(t *testing.T) {
			// Infer type
			dt, err := inferArrowType(val)
			if err != nil {
				t.Fatalf("inferArrowType failed: %v", err)
			}

			// Create builder
			builder := array.NewBuilder(mem, dt)
			defer builder.Release()

			// Append value
			if err := appendValueToBuilder(builder, val); err != nil {
				t.Fatalf("appendValueToBuilder failed: %v", err)
			}

			// Build array
			arr := builder.NewArray()
			defer arr.Release()

			if arr.Len() != 1 {
				t.Errorf("expected 1 value, got %d", arr.Len())
			}
		})
	}
}
