# Upgrading from gospice v8 to v9

gospice v9 is a new major version. Unlike v8 (which was backward compatible with v7), **v9 contains breaking changes**. This guide covers everything you need to migrate.

## Summary of breaking changes

1. **Module import path** changes from `/v8` to `/v9`.
2. **`Query()` and `QueryWithParams()` are now asynchronous** and return an `*AsyncQuery` handle instead of an `array.RecordReader`.
3. **Minimum Go version is now 1.25** (was 1.24).
4. **Apache Arrow upgraded to v18.6.0** and **ADBC to v1.11.0**, matching the Spice.ai runtime's DataFusion 54.

## 1. Update the import path

```bash
go get github.com/spiceai/gospice/v9@latest
go mod tidy
```

```go
// Before
import "github.com/spiceai/gospice/v8"

// After
import "github.com/spiceai/gospice/v9"
```

## 2. `Query` / `QueryWithParams` are now asynchronous

In v8, `Query` and `QueryWithParams` were synchronous aliases for `Sql` and `SqlWithParams`. In v9 they submit the query for **asynchronous** execution on the Spice runtime and return an `*AsyncQuery` handle.

### If you want synchronous behavior (most common)

Switch to `Sql` / `SqlWithParams`, which stream results in real time and are unchanged from v8:

```go
// Before (v8) — Query returned a reader
reader, err := spice.Query(ctx, "SELECT * FROM taxi_trips LIMIT 10")
if err != nil {
    return err
}
defer reader.Release()

// After (v9) — use Sql for the same synchronous behavior
reader, err := spice.Sql(ctx, "SELECT * FROM taxi_trips LIMIT 10")
if err != nil {
    return err
}
defer reader.Release()
```

The same applies to `QueryWithParams` → `SqlWithParams`.

### If you want asynchronous behavior

`Query` / `QueryWithParams` now return an `*AsyncQuery` handle. Async queries require the runtime to be running in distributed/scheduler mode (`spiced --role scheduler` with `runtime.scheduler.state_location` configured).

```go
query, err := spice.Query(ctx, "SELECT * FROM taxi_trips")
if err != nil {
    return err
}

// Wait for completion and fetch results
reader, err := query.Results(ctx)
if err != nil {
    return err
}
defer reader.Release()

for reader.Next() {
    fmt.Println(reader.RecordBatch())
}
```

The `*AsyncQuery` handle provides:

| Method          | Description                                                     |
| --------------- | --------------------------------------------------------------- |
| `ID()`          | The server-assigned query ID.                                   |
| `Status(ctx)`   | Poll the current status once.                                   |
| `Wait(ctx)`     | Block until the query reaches a terminal status.                |
| `Results(ctx)`  | Wait for completion and return an `array.RecordReader`.         |
| `Cancel(ctx)`   | Request cancellation of a running query.                        |

Status values are `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, and `CLOSED`.

## 3. Go version

gospice v9 requires **Go 1.25+**.

Check your Go version:

```bash
go version
```

If you are on an older version, [install Go 1.25 or newer](https://go.dev/dl/).

## 4. Dependencies

gospice v9 upgrades its Apache Arrow dependencies to match the Spice.ai runtime built on DataFusion 54:

- `github.com/apache/arrow-go/v18` → `v18.6.0`
- `github.com/apache/arrow-adbc/go/adbc` → `v1.11.0`

Running `go mod tidy` after updating the import path will pull these in automatically.

## Unchanged APIs

Everything else is source-compatible with v8, including:

- `Sql()` and `SqlWithParams()`
- `IsSpiceHealthy()` and `IsSpiceReady()`
- Client initialization (`NewSpiceClient`, `Init`, `WithApiKey`, `WithSpiceCloudAddress`, `WithFlightAddress`, `WithHttpAddress`, mTLS options)
- `RefreshDataset()` and the typed parameter constructors
