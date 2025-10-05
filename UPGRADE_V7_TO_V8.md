# Upgrading to gospice v8

## Overview

gospice v8 is a major version update that introduces:

- New cleaner API with `Sql()` and `SqlWithParams()` methods
- Health check methods `IsSpiceHealthy()` and `IsSpiceReady()`
- Upgraded to Apache Arrow v18 and Go 1.24

**Good news:** v8 is fully backward compatible with v7. Your existing `Query()` and `QueryWithParams()` calls will continue to work!

## Quick Upgrade

### 1. Update Module

```bash
go get github.com/spiceai/gospice/v8@latest
go mod tidy
```

### 2. Update Imports

Update all imports in your Go files:

```go
// Before (v7)
import "github.com/spiceai/gospice/v7"

// After (v8)
import "github.com/spiceai/gospice/v8"
```

**Find and replace:**

```bash
# On Linux/macOS
find . -name "*.go" -type f -exec sed -i 's|github.com/spiceai/gospice/v7|github.com/spiceai/gospice/v8|g' {} +

# On macOS with BSD sed
find . -name "*.go" -type f -exec sed -i '' 's|github.com/spiceai/gospice/v7|github.com/spiceai/gospice/v8|g' {} +
```

### 3. Verify Build

```bash
go build ./...
go test ./...
```

That's it! Your code should work without any other changes.

## What's New in v8

### 1. New Method Names (Recommended)

gospice v8 introduces cleaner method names:

```go
// Old way (still works for backward compatibility)
reader, err := spice.Query(ctx, "SELECT * FROM users")
reader, err := spice.QueryWithParams(ctx, "SELECT * FROM users WHERE id = $1", userId)

// New way (recommended)
reader, err := spice.Sql(ctx, "SELECT * FROM users")
reader, err := spice.SqlWithParams(ctx, "SELECT * FROM users WHERE id = $1", userId)
```

**Why the change?**

- Shorter, cleaner method names
- Better alignment with SQL standard naming
- Your old code continues to work - `Query()` and `QueryWithParams()` are maintained for backward compatibility

**Supported parameter types:**

- Integers: `int32`, `int64`, `uint32`, `uint64`
- Floats: `float32`, `float64`
- String: `string`
- Boolean: `bool`
- Binary: `[]byte`

### 2. Health Check Methods

Check Spice instance health before making queries:

```go
ctx := context.Background()

// Check if instance is healthy (no auth required)
if !spice.IsSpiceHealthy(ctx) {
    log.Println("Spice instance is not healthy")
    return
}

// Check if instance is ready (requires API key for cloud)
if !spice.IsSpiceReady(ctx) {
    log.Println("Spice instance is not ready")
    return
}

// Proceed with queries...
```

**Use cases:**

- Application startup validation
- Health monitoring and alerting
- Graceful test skipping in CI/CD
- Connection readiness checks

### 4. ADBC Support

gospice v8 uses ADBC under the hood for better performance:

- **Lazy initialization** - ADBC client initializes when first used
- **Automatic retries** - Built-in retry logic for transient failures
- **Better error handling** - More detailed error messages
- **Native Arrow integration** - Improved performance with Arrow data

No code changes needed - it works automatically!

## Migration Scenarios

### Scenario 1: Basic Query Application

**Before (v7):**

```go
import "github.com/spiceai/gospice/v7"

spice := gospice.NewSpiceClient()
defer spice.Close()

spice.Init(gospice.WithApiKey(apiKey), gospice.WithSpiceCloudAddress())

reader, err := spice.Query(ctx, "SELECT * FROM table LIMIT 10")
```

**After (v8):**

```go
import "github.com/spiceai/gospice/v8"

spice := gospice.NewSpiceClient()
defer spice.Close()

spice.Init(gospice.WithApiKey(apiKey), gospice.WithSpiceCloudAddress())

// Same code works! Or use new parameterized queries:
reader, err := spice.QueryWithParams(ctx, "SELECT * FROM table LIMIT 10")
```

### Scenario 2: Queries with User Input

**Before (v7) - Vulnerable:**

```go
import "github.com/spiceai/gospice/v7"

// ⚠️ SQL injection risk!
userId := request.GetUserId()
sql := fmt.Sprintf("SELECT * FROM users WHERE id = %s", userId)
reader, err := spice.Query(ctx, sql)
```

**After (v8) - Secure:**

```go
import "github.com/spiceai/gospice/v8"

// ✅ Protected from SQL injection
userId := request.GetUserId()
reader, err := spice.QueryWithParams(ctx,
    "SELECT * FROM users WHERE id = $1",
    userId,
)
```

### Scenario 3: Testing with Health Checks

**Before (v7):**

```go
import "github.com/spiceai/gospice/v7"

func TestQueryData(t *testing.T) {
    spice := gospice.NewSpiceClient()
    defer spice.Close()
    spice.Init()

    // Test might fail if Spice is not running
    reader, err := spice.Query(ctx, "SELECT * FROM data")
    if err != nil {
        t.Fatalf("Query failed: %v", err)
    }
}
```

**After (v8) - Graceful:**

```go
import "github.com/spiceai/gospice/v8"

func TestQueryData(t *testing.T) {
    spice := gospice.NewSpiceClient()
    defer spice.Close()
    spice.Init()

    // Skip gracefully if Spice is not available
    ctx := context.Background()
    if !spice.IsSpiceHealthy(ctx) {
        t.Skip("Spice instance not available")
    }

    reader, err := spice.Query(ctx, "SELECT * FROM data")
    if err != nil {
        t.Fatalf("Query failed: %v", err)
    }
}
```

## Dependency Updates

### Apache Arrow v17 → v18

If you directly use Apache Arrow types, update imports:

```go
// Before (v17)
import "github.com/apache/arrow/go/v17/arrow"
import "github.com/apache/arrow/go/v17/arrow/array"

// After (v18)
import "github.com/apache/arrow-go/v18/arrow"
import "github.com/apache/arrow-go/v18/arrow/array"
```

**Update command:**

```bash
go get github.com/apache/arrow-go/v18@latest
```

### Go Version

gospice v8 requires **Go 1.24+**

Check your Go version:

```bash
go version
```

Update if needed:

```bash
# Download from https://go.dev/dl/
# Or use your package manager
```

## Troubleshooting

### Error: "cannot find module github.com/spiceai/gospice/v8"

Make sure you've updated go.mod:

```bash
go get github.com/spiceai/gospice/v8@latest
go mod tidy
```

### Error: "ADBC client is not initialized"

This can happen if ADBC initialization fails. The client will retry automatically. If the error persists:

1. Check your network connection
2. Verify API key (if using Spice Cloud)
3. Ensure Spice runtime is running (if using local)
4. Check firewall rules

### Arrow Import Errors

If you see errors like `cannot find package "github.com/apache/arrow/go/v17"`:

```bash
# Update Arrow to v18
go get github.com/apache/arrow-go/v18@latest
go mod tidy

# Update imports in your code
# Change: github.com/apache/arrow/go/v17
# To:     github.com/apache/arrow-go/v18
```

### Tests Failing in CI/CD

Use health checks to skip tests when Spice is not available:

```go
if !spice.IsSpiceHealthy(ctx) {
    t.Skip("Skipping - Spice instance not available")
}
```

Or set up environment-specific test suites:

```bash
# Run only unit tests (no Spice required)
go test -run TestUnit ./...

# Run integration tests (requires Spice)
go test -run TestIntegration ./...
```

## Best Practices

### 1. Always Use Parameterized Queries for User Input

```go
// ❌ Don't do this with user input
userId := getUserInput()
sql := fmt.Sprintf("SELECT * FROM users WHERE id = %s", userId)
reader, err := spice.Query(ctx, sql)

// ✅ Do this instead
reader, err := spice.QueryWithParams(ctx,
    "SELECT * FROM users WHERE id = $1",
    userId,
)
```

### 2. Add Health Checks to Applications

```go
func main() {
    spice := gospice.NewSpiceClient()
    defer spice.Close()

    if err := spice.Init(...); err != nil {
        log.Fatal(err)
    }

    // Wait for Spice to be ready
    ctx := context.Background()
    for i := 0; i < 30; i++ {
        if spice.IsSpiceHealthy(ctx) {
            log.Println("Spice is ready!")
            break
        }
        log.Println("Waiting for Spice...")
        time.Sleep(2 * time.Second)
    }

    // Start application...
}
```

### 3. Use Context with Timeout

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

reader, err := spice.QueryWithParams(ctx, sql, params...)
```

### 4. Handle Errors Properly

```go
reader, err := spice.QueryWithParams(ctx, sql, param1, param2)
if err != nil {
    log.Printf("Query failed: %v", err)
    // Handle error appropriately
    return
}
defer reader.Release()

for reader.Next() {
    record := reader.Record()
    defer record.Release()
    // Process record...
}

// Check for errors during iteration
if err := reader.Err(); err != nil {
    log.Printf("Error reading results: %v", err)
}
```

## Getting Help

- 📖 **Documentation**: [docs.spice.ai/sdks/go](https://docs.spice.ai/sdks/go)
- 💬 **Discord**: [spice.ai/discord](https://spice.ai/discord)
- 🐛 **Issues**: [github.com/spiceai/gospice/issues](https://github.com/spiceai/gospice/issues)
- 📚 **Examples**: See `cmd/main.go` in the repository

## Summary Checklist

- [ ] Run `go get github.com/spiceai/gospice/v8@latest`
- [ ] Update imports from `v7` to `v8`
- [ ] Run `go mod tidy`
- [ ] Update Arrow imports if using Arrow types directly
- [ ] Verify build with `go build ./...`
- [ ] Run tests with `go test ./...`
- [ ] Consider using `QueryWithParams()` for queries with user input
- [ ] Add health checks to tests and applications
- [ ] Update CI/CD pipelines if needed

Welcome to gospice v8! 🚀
