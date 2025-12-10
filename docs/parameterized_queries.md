# Parameterized Query Type System

This document describes the comprehensive type system for parameterized queries in gospice, including support for all Arrow types and explicit type annotation.

## Overview

The parameterized query system supports three modes of parameter usage:

1. **Type Inference**: Pass Go values directly, types are automatically inferred
2. **Explicit Types**: Use `Param` struct with explicit Arrow type
3. **Helper Functions**: Use convenience functions for common types

## Basic Usage

### Simple Type Inference

```go
// Types are automatically inferred from Go values
reader, err := client.SqlWithParams(ctx,
    "SELECT * FROM table WHERE id = $1 AND name = $2",
    42,      // Inferred as int64
    "test",  // Inferred as String
)
```

### Explicit Type Annotation

```go
// Explicitly specify Arrow types for precise control
reader, err := client.SqlWithParams(ctx,
    "SELECT * FROM table WHERE id = $1 AND created = $2",
    gospice.Int32Param(42),
    gospice.TimestampParam(ts, arrow.Microsecond, "UTC"),
)
```

## Supported Types

### Primitive Integer Types

| Go Type  | Arrow Type | Helper Function  | Auto-Inferred |
| -------- | ---------- | ---------------- | ------------- |
| `int8`   | Int8       | `Int8Param(v)`   | ✅            |
| `int16`  | Int16      | `Int16Param(v)`  | ✅            |
| `int32`  | Int32      | `Int32Param(v)`  | ✅            |
| `int64`  | Int64      | `Int64Param(v)`  | ✅            |
| `uint8`  | Uint8      | `Uint8Param(v)`  | ✅            |
| `uint16` | Uint16     | `Uint16Param(v)` | ✅            |
| `uint32` | Uint32     | `Uint32Param(v)` | ✅            |
| `uint64` | Uint64     | `Uint64Param(v)` | ✅            |

### Floating Point Types

| Go Type         | Arrow Type | Helper Function   | Auto-Inferred |
| --------------- | ---------- | ----------------- | ------------- |
| `uint16` (bits) | Float16    | `Float16Param(v)` | ❌            |
| `float32`       | Float32    | `Float32Param(v)` | ✅            |
| `float64`       | Float64    | `Float64Param(v)` | ✅            |

### String and Binary Types

| Go Type  | Arrow Type      | Helper Function                  | Auto-Inferred |
| -------- | --------------- | -------------------------------- | ------------- |
| `string` | String          | `StringParam(v)`                 | ✅            |
| `string` | LargeString     | `LargeStringParam(v)`            | ❌            |
| `[]byte` | Binary          | `BinaryParam(v)`                 | ✅            |
| `[]byte` | LargeBinary     | `LargeBinaryParam(v)`            | ❌            |
| `[]byte` | FixedSizeBinary | `FixedSizeBinaryParam(v, width)` | ❌            |
| `bool`   | Boolean         | `BoolParam(v)`                   | ✅            |

### Temporal Types

| Arrow Type | Helper Function               | Example                                                                       |
| ---------- | ----------------------------- | ----------------------------------------------------------------------------- |
| Date32     | `Date32Param(v)`              | `Date32Param(arrow.Date32(18628))`                                            |
| Date64     | `Date64Param(v)`              | `Date64Param(arrow.Date64(1609459200000))`                                    |
| Time32     | `Time32Param(v, unit)`        | `Time32Param(arrow.Time32(43200), arrow.Second)`                              |
| Time64     | `Time64Param(v, unit)`        | `Time64Param(arrow.Time64(43200000), arrow.Microsecond)`                      |
| Timestamp  | `TimestampParam(v, unit, tz)` | `TimestampParam(arrow.Timestamp(1609459200000000), arrow.Microsecond, "UTC")` |
| Duration   | `DurationParam(v, unit)`      | `DurationParam(arrow.Duration(1000000), arrow.Microsecond)`                   |

### Interval Types

| Arrow Type           | Helper Function                | Example                                                                                        |
| -------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------- |
| MonthInterval        | `MonthIntervalParam(v)`        | `MonthIntervalParam(arrow.MonthInterval(12))`                                                  |
| DayTimeInterval      | `DayTimeIntervalParam(v)`      | `DayTimeIntervalParam(arrow.DayTimeInterval{Days: 1, Milliseconds: 1000})`                     |
| MonthDayNanoInterval | `MonthDayNanoIntervalParam(v)` | `MonthDayNanoIntervalParam(arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: 3000})` |

### Decimal Types

| Arrow Type | Helper Function                   | Example                                  |
| ---------- | --------------------------------- | ---------------------------------------- |
| Decimal128 | `Decimal128Param(v, prec, scale)` | `Decimal128Param([16]byte{...}, 38, 10)` |
| Decimal256 | `Decimal256Param(v, prec, scale)` | `Decimal256Param([32]byte{...}, 76, 10)` |

### Special Types

| Arrow Type | Helper Function | Example       |
| ---------- | --------------- | ------------- |
| Null       | `NullParam()`   | `NullParam()` |

## API Methods

### SqlWithParams

Primary method for parameterized queries:

```go
func (c *SpiceClient) SqlWithParams(ctx context.Context, sql string, params ...interface{}) (array.RecordReader, error)
```

**Parameters:**

- `ctx`: Context for the query
- `sql`: SQL query with positional placeholders (`$1`, `$2`, etc.)
- `params`: Variable number of parameters (Go values, Param structs, or Arrow types)

**Returns:**

- `array.RecordReader`: Arrow RecordReader for results
- `error`: Error if query fails

### QueryWithParams

Alias for `SqlWithParams` for backward compatibility:

```go
func (c *SpiceClient) QueryWithParams(ctx context.Context, sql string, params ...interface{}) (array.RecordReader, error)
```

## Examples

### Example 1: Basic Query with Inferred Types

```go
package main

import (
    "context"
    "fmt"
    gospice "github.com/spiceai/gospice/v8"
)

func main() {
    client := gospice.NewSpiceClient()
    defer client.Close()

    if err := client.Init(); err != nil {
        panic(err)
    }

    // Simple query with inferred types
    reader, err := client.SqlWithParams(context.Background(),
        "SELECT * FROM customers WHERE age > $1 AND country = $2 LIMIT $3",
        18,      // int -> Int64
        "USA",   // string -> String
        100,     // int -> Int64
    )
    if err != nil {
        panic(err)
    }
    defer reader.Release()

    // Process results
    for reader.Next() {
        record := reader.RecordBatch()
        fmt.Println(record)
        record.Release()
    }
}
```

### Example 2: Explicit Types for Precision

```go
package main

import (
    "context"
    "github.com/apache/arrow-go/v18/arrow"
    gospice "github.com/spiceai/gospice/v8"
)

func main() {
    client := gospice.NewSpiceClient()
    defer client.Close()

    if err := client.Init(); err != nil {
        panic(err)
    }

    // Use explicit types when precision matters
    reader, err := client.SqlWithParams(context.Background(),
        "SELECT * FROM orders WHERE order_id = $1 AND quantity = $2",
        gospice.Int32Param(12345),      // Explicitly Int32
        gospice.Uint16Param(10),         // Explicitly Uint16
    )
    if err != nil {
        panic(err)
    }
    defer reader.Release()

    // Process results...
}
```

### Example 3: Temporal Data

```go
package main

import (
    "context"
    "time"
    "github.com/apache/arrow-go/v18/arrow"
    gospice "github.com/spiceai/gospice/v8"
)

func main() {
    client := gospice.NewSpiceClient()
    defer client.Close()

    if err := client.Init(); err != nil {
        panic(err)
    }

    // Query with timestamp
    startTime := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
    timestamp := arrow.Timestamp(startTime.UnixMicro())

    reader, err := client.SqlWithParams(context.Background(),
        "SELECT * FROM events WHERE created_at > $1",
        gospice.TimestampParam(timestamp, arrow.Microsecond, "UTC"),
    )
    if err != nil {
        panic(err)
    }
    defer reader.Release()

    // Process results...
}
```

### Example 4: Mixed Inferred and Explicit Types

```go
reader, err := client.SqlWithParams(ctx,
    "SELECT * FROM table WHERE id = $1 AND name = $2 AND created = $3 AND active = $4",
    42,                                    // Inferred as Int64
    gospice.StringParam("test"),          // Explicit String
    gospice.Date32Param(arrow.Date32(18628)), // Explicit Date32
    true,                                  // Inferred as Boolean
)
```

### Example 5: Large String Data

```go
// When working with very large strings (>2GB), use LargeString
largeText := ... // Large string data
reader, err := client.SqlWithParams(ctx,
    "SELECT * FROM documents WHERE content = $1",
    gospice.LargeStringParam(largeText),
)
```

### Example 6: Decimal Precision

```go
// Working with high-precision decimal numbers
var decimalBytes [16]byte
// ... populate decimal bytes ...

reader, err := client.SqlWithParams(ctx,
    "SELECT * FROM financial WHERE amount = $1",
    gospice.Decimal128Param(decimalBytes, 38, 10), // 38 precision, 10 scale
)
```

## Custom Type Annotation

For advanced use cases, you can create custom `Param` structs:

```go
// Create a custom param with explicit type
customParam := gospice.NewTypedParam(
    myValue,
    &arrow.TimestampType{
        Unit: arrow.Nanosecond,
        TimeZone: "America/New_York",
    },
)

reader, err := client.SqlWithParams(ctx,
    "SELECT * FROM table WHERE ts = $1",
    customParam,
)
```

## Type Inference Rules

When types are not explicitly specified:

1. **Integers**: `int` → `Int64`, `uint` → `Uint64` (platform-safe defaults)
2. **Floats**: `float32` → `Float32`, `float64` → `Float64`
3. **Strings**: `string` → `String` (standard UTF-8)
4. **Binary**: `[]byte` → `Binary` (variable-length)
5. **Temporal**: Arrow temporal types → defaults with sensible units
   - `Time32` → milliseconds
   - `Time64` → microseconds
   - `Timestamp` → microseconds with UTC
   - `Duration` → microseconds
6. **Null**: `nil` → `Null`

## Best Practices

1. **Use Type Inference for Simple Cases**: For common types like int, string, bool
2. **Use Explicit Types for Precision**: When exact type matters (e.g., Int32 vs Int64)
3. **Use Temporal Helpers for Dates/Times**: Ensures correct units and timezones
4. **Use LargeString/LargeBinary**: For data >2GB
5. **Use Decimals for Financial Data**: Precise decimal arithmetic
6. **Always Close Readers**: Use `defer reader.Release()` after query

## Error Handling

The system provides clear error messages for type mismatches:

```go
reader, err := client.SqlWithParams(ctx, "SELECT $1", unsupportedType)
if err != nil {
    // Error will indicate: "unsupported parameter type: <type>"
    log.Printf("Query failed: %v", err)
}
```

## Performance Considerations

1. **Type Inference**: Minimal overhead, types are determined once per query
2. **Explicit Types**: No overhead, types are directly specified
3. **Arrow Conversion**: Zero-copy when possible, efficient serialization
4. **Large Data**: Use Large variants (LargeString, LargeBinary) for >2GB data

## Migration Guide

### From Previous Versions

If you were using the previous parameterized query API, no changes are required:

```go
// Old code still works
reader, err := client.QueryWithParams(ctx, "SELECT $1", 42)

// New equivalent (preferred)
reader, err := client.SqlWithParams(ctx, "SELECT $1", 42)
```

### Adding Explicit Types

To add explicit type control to existing code:

```go
// Before
reader, err := client.SqlWithParams(ctx, "SELECT $1, $2", 42, "test")

// After (with explicit types)
reader, err := client.SqlWithParams(ctx, "SELECT $1, $2",
    gospice.Int32Param(42),
    gospice.StringParam("test"),
)
```

## Troubleshooting

### Type Mismatch Errors

If you get type mismatch errors, use explicit types:

```go
// If inference picks wrong type
reader, err := client.SqlWithParams(ctx, "SELECT $1", int32(42))  // Might infer as Int64

// Use explicit type instead
reader, err := client.SqlWithParams(ctx, "SELECT $1", gospice.Int32Param(42))
```

### Unsupported Type Error

If you encounter "unsupported parameter type", use `NewTypedParam` with explicit Arrow type:

```go
param := gospice.NewTypedParam(value, myCustomArrowType)
reader, err := client.SqlWithParams(ctx, "SELECT $1", param)
```
