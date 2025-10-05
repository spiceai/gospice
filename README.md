# gospice

Golang SDK for Spice.ai

See Go Docs at [pkg.go.dev/github.com/spiceai/gospice/v8](https://pkg.go.dev/github.com/spiceai/gospice/v8).

For full documentation visit [docs.spice.ai](https://docs.spice.ai/sdks/go).

## Usage

1. Get the gospice package.

```go
go get github.com/spiceai/gospice/v8
```

1. Import the package.

```go
import "github.com/spiceai/gospice/v8"
```

1. Create a SpiceClient passing in your API key. Get your free API key at [spice.ai](https://spice.ai).

```go
spice := NewSpiceClient()
defer spice.Close()
```

1. Initialize the SpiceClient with spice.ai cloud.

```go
if err := spice.Init(
    spice.WithApiKey(ApiKey),
    spice.WithSpiceCloudAddress()
); err != nil {
    panic(fmt.Errorf("error initializing SpiceClient: %w", err))
}
```

1. Execute a query and get back an Apache Arrow Reader.

```go
    reader, err := spice.Query(context.Background(), "SELECT 1")
    if err != nil {
        panic(fmt.Errorf("error querying: %w", err))
    }
    defer reader.Release()
```

1. Iterate through the reader to access the records.

```go
    for reader.Next() {
        record := reader.Record()
        defer record.Release()
        fmt.Println(record)
    }
```

### Using Parameterized Queries (Recommended)

gospice v8 supports parameterized queries using ADBC (Arrow Database Connectivity), which is the recommended approach for queries with parameters to prevent SQL injection:

```go
// Query with a single parameter
reader, err := spice.QueryWithParams(
    context.Background(),
    "SELECT * FROM tpch.customer WHERE c_custkey > $1 LIMIT 10",
    100,
)
if err != nil {
    panic(fmt.Errorf("error querying: %w", err))
}
defer reader.Release()

for reader.Next() {
    record := reader.Record()
    defer record.Release()
    fmt.Println(record)
}
```

Query with multiple parameters:

```go
reader, err := spice.QueryWithParams(
    context.Background(),
    "SELECT * FROM taxi_trips WHERE trip_distance > $1 AND fare_amount > $2 LIMIT 100",
    5.0,
    20.0,
)
if err != nil {
    panic(fmt.Errorf("error querying: %w", err))
}
defer reader.Release()
```

Supported parameter types:

- Integers: `int`, `int8`, `int16`, `int32`, `int64`, `uint`, `uint8`, `uint16`, `uint32`, `uint64`
- Floating point: `float32`, `float64`
- String: `string`
- Boolean: `bool`
- Binary: `[]byte`

### Using local spice runtime

Follow the [quickstart guide](https://github.com/spiceai/spiceai?tab=readme-ov-file#%EF%B8%8F-quickstart-local-machine) to install and run spice locally

Initialize the SpiceClient to use local runtime connection:

```go
if err := spice.Init(); err != nil {
    panic(fmt.Errorf("error initializing SpiceClient: %w", err))
}
```

Configure with a custom flight address:

```go
if err := spice.Init(
    spice.WithFlightAddress("grpc://localhost:50052")
); err != nil {
    panic(fmt.Errorf("error initializing SpiceClient: %w", err))
}
```

## Health Checks

gospice v8 provides health check methods to verify Spice instance status before executing queries:

```go
// Check if Spice instance is healthy (unauthenticated)
ctx := context.Background()
if !spice.IsSpiceHealthy(ctx) {
    log.Println("Spice instance is not healthy")
    return
}

// Check if Spice Cloud is ready (requires API key)
if !spice.IsSpiceReady(ctx) {
    log.Println("Spice Cloud is not ready or API key is invalid")
    return
}
```

- `IsSpiceHealthy(ctx)` - Calls `/health` endpoint (unauthenticated)
- `IsSpiceReady(ctx)` - Calls `/v1/ready` endpoint (requires API key)

## Example

Run `go run .` to execute a sample query and print the results to the console.

See [query_test.go](query_test.go) for examples on querying TPC-H and taxi trips datasets.

### Connection retry

The `SpiceClient` implements connection retry mechanism (3 attempts by default).
The number of attempts can be configured via `SetMaxRetries`:

```go
spice := NewSpiceClient()
spice.SetMaxRetries(5) // Setting to 0 will disable retries
```

Retries are performed for connection and system internal errors. It is the SDK user's responsibility to properly
handle other errors, for example RESOURCE_EXHAUSTED (HTTP 429).

## Upgrading from v7 to v8

gospice v8 is fully backward compatible with v7. To upgrade:

```bash
go get github.com/spiceai/gospice/v8@latest
go mod tidy
```

Update your imports:

```go
// Before
import "github.com/spiceai/gospice/v7"

// After
import "github.com/spiceai/gospice/v8"
```

**What's new in v8:**

- New `Sql()` and `SqlWithParams()` methods for cleaner API (`.Query()` methods still work for backward compatibility)
- `IsSpiceHealthy()` and `IsSpiceReady()` health check methods
- Apache Arrow v18 and Go 1.24 support

See [UPGRADE_V7_TO_V8.md](UPGRADE_V7_TO_V8.md) for detailed migration guide.

## Testing and Benchmarking

### Running Tests

Run all tests:

```bash
go test ./...
```

Run tests with verbose output:

```bash
go test -v ./...
```

Run specific test suites:

```bash
# Local runtime tests only
go test -v -run="TestLocal"

# Cloud tests only
go test -v -run="TestCloud"

# ADBC tests only
go test -v -run="TestADBC"
```

### Running Benchmarks

Run all benchmarks:

```bash
go test -bench=. -benchmem
```

Run specific benchmarks:

```bash
# Benchmark query performance
go test -bench=BenchmarkQuery -benchmem

# Benchmark parameterized queries
go test -bench=BenchmarkQueryWithParams -benchmem

# Benchmark health checks
go test -bench=BenchmarkHealthChecks -benchmem

# Benchmark client initialization
go test -bench=BenchmarkClientInitialization -benchmem
```

Run benchmarks with custom settings:

```bash
# Run for 10 seconds each
go test -bench=. -benchtime=10s

# Run with CPU profiling
go test -bench=. -cpuprofile=cpu.prof

# Run with memory profiling
go test -bench=. -memprofile=mem.prof
```

Available benchmarks:

- `BenchmarkCloudQuery` - Basic query performance against Spice Cloud
- `BenchmarkCloudQueryWithParams` - Parameterized query performance (Cloud)
- `BenchmarkLocalQuery` - Query performance against local runtime
- `BenchmarkLocalQueryWithParams` - Parameterized query performance (Local)
- `BenchmarkParameterBinding` - Parameter binding overhead with varying parameter counts
- `BenchmarkClientInitialization` - Client initialization overhead
- `BenchmarkHealthChecks` - Health check endpoint performance
- `BenchmarkRecordProcessing` - Different record processing patterns
