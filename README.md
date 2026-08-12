# gospice

Golang SDK for Spice.ai

See Go Docs at [pkg.go.dev/github.com/spiceai/gospice/v9](https://pkg.go.dev/github.com/spiceai/gospice/v9).

For full documentation visit [docs.spice.ai](https://docs.spice.ai/sdks/go).

## Usage

1. Get the gospice package.

```go
go get github.com/spiceai/gospice/v9@latest
```

1. Import the package.

```go
import "github.com/spiceai/gospice/v9"
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
    reader, err := spice.Sql(context.Background(), "SELECT 1")
    if err != nil {
        panic(fmt.Errorf("error querying: %w", err))
    }
    defer reader.Release()
```

1. Iterate through the reader to access the records.

```go
    for reader.Next() {
        record := reader.RecordBatch()
        defer record.Release()
        fmt.Println(record)
    }
```

### Using Parameterized Queries (Recommended)

gospice v9 supports parameterized queries using ADBC (Arrow Database Connectivity), which is the recommended approach for queries with parameters to prevent SQL injection:

```go
// Query with a single parameter
reader, err := spice.SqlWithParams(
    context.Background(),
    "SELECT * FROM tpch.customer WHERE c_custkey > $1 LIMIT 10",
    100,
)
if err != nil {
    panic(fmt.Errorf("error querying: %w", err))
}
defer reader.Release()

for reader.Next() {
    record := reader.RecordBatch()
    defer record.Release()
    fmt.Println(record)
}
```

Query with multiple parameters:

```go
reader, err := spice.SqlWithParams(
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

**Supported parameter types with automatic type inference:**

- Integers: `int`, `int8`, `int16`, `int32`, `int64`, `uint`, `uint8`, `uint16`, `uint32`, `uint64`
- Floating point: `float32`, `float64`
- String: `string`
- Boolean: `bool`
- Binary: `[]byte`
- Null values: `nil`

**Typed Parameters for Advanced Use Cases:**

For precise control over Arrow types, use typed parameter constructors:

```go
import "github.com/spiceai/gospice/v9"

// Explicit type control for complex scenarios
reader, err := spice.SqlWithParams(
    ctx,
    "SELECT * FROM data WHERE id = $1 AND amount = $2 AND active = $3",
    gospice.Int64Param(12345),           // Explicitly int64
    gospice.Decimal128Param(...),        // Decimal with precision
    gospice.BoolParam(true),             // Explicitly boolean
)
```

Available typed parameter constructors:

- **Integers**: `Int8Param`, `Int16Param`, `Int32Param`, `Int64Param`, `Uint8Param`, `Uint16Param`, `Uint32Param`, `Uint64Param`
- **Floating point**: `Float16Param`, `Float32Param`, `Float64Param`
- **Strings**: `StringParam`, `LargeStringParam`
- **Binary**: `BinaryParam`, `LargeBinaryParam`, `FixedSizeBinaryParam`
- **Boolean**: `BoolParam`
- **Date/Time**: `Date32Param`, `Date64Param`, `Time32Param`, `Time64Param`, `TimestampParam`, `DurationParam`
- **Intervals**: `MonthIntervalParam`, `DayTimeIntervalParam`, `MonthDayNanoIntervalParam`
- **Decimals**: `Decimal128Param`, `Decimal256Param`
- **Null**: `NullParam`

Or use the generic constructors:

- `NewParam(value)` - Creates a parameter with automatic type inference
- `NewTypedParam(value, arrowType)` - Creates a parameter with explicit Arrow type

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

## Async Queries

`Query` and `QueryWithParams` submit SQL for **asynchronous** execution and return an `*AsyncQuery` handle. Async queries run in the background on the Spice runtime and are designed for long-running analytical and batch workloads. They require the runtime to be running in distributed/scheduler mode (`spiced --role scheduler` with `runtime.scheduler.state_location` configured).

```go
// Submit a query for async execution
query, err := spice.Query(context.Background(), "SELECT * FROM taxi_trips")
if err != nil {
    panic(fmt.Errorf("error submitting query: %w", err))
}
fmt.Println("query id:", query.ID())

// Wait for completion and fetch results as an Apache Arrow reader
reader, err := query.Results(context.Background())
if err != nil {
    panic(fmt.Errorf("error fetching results: %w", err))
}
defer reader.Release()

for reader.Next() {
    fmt.Println(reader.RecordBatch())
}
```

Parameterized async queries bind positional parameters (`$1`, `$2`, ...):

```go
query, err := spice.QueryWithParams(
    context.Background(),
    "SELECT * FROM taxi_trips WHERE trip_distance > $1 LIMIT $2",
    5.0,
    100,
)
```

The `*AsyncQuery` handle provides:

- `ID()` - the server-assigned query ID
- `Status(ctx)` - poll the current status once (`PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `CLOSED`)
- `Wait(ctx)` - block until the query reaches a terminal status
- `Results(ctx)` - wait for completion and return results as an `array.RecordReader`
- `Cancel(ctx)` - request cancellation

For synchronous, real-time streaming queries, use `Sql` / `SqlWithParams` instead.

## Health Checks

gospice v9 provides health check methods to verify Spice instance status before executing queries:

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

### Runtime Status

`IsSpiceReady` collapses the whole runtime into a single boolean. When you need to know
*which* component is not ready, use `RuntimeStatus` to get per-connection detail:

```go
details, err := spice.RuntimeStatus(ctx)
if err != nil {
    log.Fatalf("error getting runtime status: %v", err)
}

for _, d := range details {
    fmt.Printf("%s (%s): %s\n", d.Name, d.Endpoint, d.Status)
}
// http (127.0.0.1:8090): Ready
// flight (127.0.0.1:50051): Ready
// metrics (N/A): Disabled
// opentelemetry (127.0.0.1:50051): Ready
```

Each `ConnectionDetails` carries the component `Name` (`http`, `flight`, `metrics` or
`opentelemetry`), its `Endpoint`, and its `Status` — one of `Initializing`, `Ready`,
`Disabled`, `Error`, `Refreshing`, `ShuttingDown` or `NotLoaded`. `d.IsReady()` is a
shorthand for `d.Status == ComponentStatusReady`.
## Search

`Search` finds documents similar to a piece of text, using the runtime's `/v1/search` endpoint. It runs against datasets that have an embedding column and a loaded embedding model — see [Search & Retrieval](https://docs.spice.ai/features/search-and-retrieval) for how to configure them.

```go
ctx := context.Background()
limit := 3

resp, err := spice.Search(ctx, &gospice.SearchRequest{
    Text:              "tokyo plane tickets",
    Datasets:          []string{"app_messages"},
    Limit:             &limit,
    AdditionalColumns: []string{"timestamp"},
})
if err != nil {
    log.Fatalf("search failed: %v", err)
}

fmt.Printf("%d matches in %dms\n", len(resp.Results), resp.DurationMs)
for _, match := range resp.Results {
    fmt.Println(match.Score, match.Dataset, match.Matches, match.Data)
}
```

`SearchRequest` fields:

- `Text` (required) - The text to find similar documents for.
- `Datasets` - Datasets to search. Leave empty to search every searchable dataset.
- `Limit` - Maximum matches to return per dataset.
- `Where` - A SQL predicate filtering candidate rows, without the leading `WHERE` — for example `"user_id = 42"`.
- `AdditionalColumns` - Extra columns to return with each match. Primary key columns are returned in `PrimaryKey`, the rest in `Data`.
- `Keywords` - Keywords for the lexical pass of a hybrid search, which the runtime combines with the vector scores into a single ranking.

Each `SearchMatch` carries `Dataset`, `Score` (higher is more similar), `Matches` (matched values keyed by source column — a slice per column, since one column can contribute several chunks to a match), `PrimaryKey`, `Data`, and `Metadata`.

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

## Upgrading from v8 to v9

gospice v9 is a new major version with breaking changes. To upgrade:

```bash
go get github.com/spiceai/gospice/v9@latest
go mod tidy
```

Update your imports:

```go
// Before
import "github.com/spiceai/gospice/v8"

// After
import "github.com/spiceai/gospice/v9"
```

**Breaking changes in v9:**

- `Query()` and `QueryWithParams()` are now **asynchronous** and return an `*AsyncQuery` handle instead of an `array.RecordReader`. Use the handle's `Wait()` / `Results()` methods (see [Async Queries](#async-queries)), or switch to the synchronous `Sql()` / `SqlWithParams()` methods.
- Minimum Go version is now **1.25** (was 1.24).
- Upgraded to Apache Arrow v18.6.0 and ADBC v1.11.0, matching the Spice.ai runtime's DataFusion 54.

See [UPGRADE_V8_TO_V9.md](UPGRADE_V8_TO_V9.md) for the detailed migration guide.

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
