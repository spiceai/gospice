# gospice

Golang SDK for Spice.xyz

See Go Docs at [pkg.go.dev/github.com/spiceai/gospice](https://pkg.go.dev/github.com/spiceai/gospice).

For full documentation visit [docs.spice.xyz](https://docs.spice.xyz/sdks/go).

## Usage

1. Get the gospice package.

```go
go get github.com/spiceai/gospice
```

1. Create a SpiceClient passing in your API key. Get your free API key at [spice.xyz](https://spice.xyz).

```go
spice := NewSpiceClient()
defer spice.Close()
```

1. Initialize the SpiceClient.

```go
if err := spice.Init("API Key"); err != nil {
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

## Example

Run `go run .` to execute a sample query and print the results to the console.

See [client_test.go](client_test.go) for examples on querying Ethereum and Polygon blocks.
