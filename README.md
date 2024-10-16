# gospice

Golang SDK for Spice.ai

See Go Docs at [pkg.go.dev/github.com/spiceai/gospice/v6](https://pkg.go.dev/github.com/spiceai/gospice/v6).

For full documentation visit [docs.spice.ai](https://docs.spice.ai/sdks/go).

## Usage

1. Get the gospice package.

```go
go get github.com/spiceai/gospice/v6
```

1. Import the package.

```go
import "github.com/spiceai/gospice/v6"
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
    reader, err := spice.Query(context.Background(), "SELECT * FROM eth.recent_blocks ORDER BY number LIMIT 10")
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

## Example

Run `go run .` to execute a sample query and print the results to the console.

See [client_test.go](client_test.go) for examples on querying Ethereum and Polygon blocks.

### Connection retry

The `SpiceClient` implements connection retry mechanism (3 attempts by default).
The number of attempts can be configured via `SetMaxRetries`:

```go
spice := NewSpiceClient()
spice.SetMaxRetries(5) // Setting to 0 will disable retries
```

Retries are performed for connection and system internal errors. It is the SDK user's responsibility to properly
handle other errors, for example RESOURCE_EXHAUSTED (HTTP 429).
