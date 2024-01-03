# gospice

Golang SDK for Spice.ai

See Go Docs at [pkg.go.dev/github.com/spiceai/gospice/v4](https://pkg.go.dev/github.com/spiceai/gospice/v4d).

For full documentation visit [docs.spice.ai](https://docs.spice.ai/sdks/go).

## Usage

> **Note**: There is a [bug in Apache Arrow](https://github.com/apache/arrow/issues/38198) v14 that causes a high rate of errors from concurrent queries. We've addressed this in our fork. To apply the fix, add the following to your `go.mod` file until the fix is released upstream:
>
> ```
> replace github.com/apache/arrow/go/v14 => github.com/spicehq/arrow/go/v14 v14.0.3-0.20240102132723-66b53585316f
> ```

1. Get the gospice package.

```go
go get github.com/spiceai/gospice/v4
```

1. Import the package.

```go
import "github.com/spiceai/gospice/v4"
```

1. Create a SpiceClient passing in your API key. Get your free API key at [spice.ai](https://spice.ai).

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
