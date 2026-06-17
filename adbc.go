package gospice

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ADBCClient wraps ADBC database and connection for Spice.ai
type ADBCClient struct {
	db   adbc.Database
	conn adbc.Connection
	mem  memory.Allocator // Reusable memory allocator for parameter binding
}

// initADBC initializes the ADBC connection
func (c *SpiceClient) initADBC() error {
	// Create reusable memory allocator
	mem := memory.NewGoAllocator()
	driver := flightsql.NewDriver(mem)

	// Format the URI correctly for ADBC FlightSQL driver
	uri := c.flightAddress
	// If it doesn't start with grpc:// or grpc+tls://, add the appropriate scheme
	if !strings.HasPrefix(uri, "grpc://") && !strings.HasPrefix(uri, "grpc+tls://") {
		// For cloud addresses (with port 443 or containing spiceai.io), use grpc+tls
		if strings.Contains(uri, ":443") || strings.Contains(uri, "spiceai.io") {
			uri = "grpc+tls://" + uri
		} else {
			uri = "grpc://" + uri
		}
	}

	// Prepare connection options
	options := map[string]string{
		adbc.OptionKeyURI: uri,
	}

	// Add authentication if available
	if c.appId != "" && c.apiKey != "" {
		options[adbc.OptionKeyUsername] = c.appId
		options[adbc.OptionKeyPassword] = c.apiKey
	}

	// Add user agent header
	if c.userAgent != "" {
		options["adbc.flight.sql.rpc.call_header.user-agent"] = c.userAgent
	}

	db, err := driver.NewDatabase(options)
	if err != nil {
		return fmt.Errorf("error creating ADBC database: %w", err)
	}

	// Create connection
	conn, err := db.Open(context.Background())
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return fmt.Errorf("error opening ADBC connection: %w (failed to close database: %v)", err, closeErr)
		}
		return fmt.Errorf("error opening ADBC connection: %w", err)
	}

	c.adbcClient = &ADBCClient{
		db:   db,
		conn: conn,
		mem:  mem,
	}

	return nil
}

// closeADBC closes the ADBC connection and database
func (c *SpiceClient) closeADBC() error {
	if c.adbcClient == nil {
		return nil
	}

	var errors []error
	if c.adbcClient.conn != nil {
		if err := c.adbcClient.conn.Close(); err != nil {
			errors = append(errors, err)
		}
	}
	if c.adbcClient.db != nil {
		if err := c.adbcClient.db.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("error closing ADBC client: %v", errors)
	}

	return nil
}

// SqlWithParams executes a parameterized SQL query against Spice.ai and returns an Apache Arrow RecordReader
// This is the recommended method for querying with parameters to prevent SQL injection
// Parameters should use positional placeholders (e.g., $1, $2) in the SQL query
//
// Parameters can be:
// - Simple Go values (int, string, bool, etc.) - type will be inferred
// - Param structs with explicit type annotation using NewTypedParam() or helper functions
// - Arrow types (arrow.Date32, arrow.Timestamp, etc.)
//
// Example:
//
//	reader, err := client.SqlWithParams(ctx, "SELECT * FROM table WHERE id = $1 AND name = $2", 123, "test")
//	reader, err := client.SqlWithParams(ctx, "SELECT * FROM table WHERE ts = $1", TimestampParam(ts, arrow.Microsecond, "UTC"))
func (c *SpiceClient) SqlWithParams(ctx context.Context, sql string, params ...any) (array.RecordReader, error) {
	if c.adbcClient == nil {
		// Try lazy initialization
		if err := c.initADBC(); err != nil {
			return nil, fmt.Errorf("ADBC client is not initialized and failed to initialize: %w", err)
		}
	}

	// Record the connection we are about to use so that, if it turns out to be
	// stale, we only re-open it once even when many goroutines hit the failure
	// at the same time.
	used := c.adbcClient

	rdr, err := c.execADBCWithBackoff(ctx, sql, params...)
	if err != nil && isADBCAuthError(err) {
		// The ADBC connection authenticates only once, when it is opened: a
		// Basic-auth handshake yields a server-side session token that is then
		// reused for every prepared statement on that connection. That session
		// can be invalidated server-side (e.g. expired after a period of
		// inactivity), after which the cached token is rejected on every
		// subsequent request and the connection cannot recover on its own.
		// Re-open the connection to perform a fresh handshake, then retry once.
		if reinitErr := c.reinitADBC(used); reinitErr != nil {
			return nil, fmt.Errorf("ADBC re-authentication failed: %w (original error: %v)", reinitErr, err)
		}
		rdr, err = c.execADBCWithBackoff(ctx, sql, params...)
	}
	if err != nil {
		return nil, err
	}

	return rdr, nil
}

// execADBCWithBackoff runs a parameterized ADBC query, retrying transient
// (e.g. Unavailable / Internal) failures with the client's backoff policy.
// Authentication failures are returned as-is (not retried here) so the caller
// can re-establish the connection before retrying.
func (c *SpiceClient) execADBCWithBackoff(ctx context.Context, sql string, params ...any) (array.RecordReader, error) {
	var rdr array.RecordReader
	err := backoff.Retry(func() error {
		var err error
		rdr, err = c.queryADBCWithParams(ctx, sql, params...)
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				switch st.Code() {
				case codes.Unavailable, codes.Unknown, codes.DeadlineExceeded, codes.Aborted, codes.Internal:
					return err
				}
				if strings.Contains(err.Error(), "malformed header: missing HTTP content-type") {
					return err
				}
				if err.Error() == "rpc error: code = Unknown desc = " {
					return err
				}
			}
			return backoff.Permanent(err)
		}
		return nil
	}, backoff.WithMaxRetries(c.backoffPolicy, uint64(c.maxRetries)))
	if err != nil {
		return nil, err
	}

	return rdr, nil
}

// isADBCAuthError reports whether err indicates the ADBC connection's
// credentials/session were rejected by the server (as opposed to a transient
// or query error). Such failures are not recoverable on the existing
// connection and require re-opening it to perform a fresh handshake.
func isADBCAuthError(err error) bool {
	if err == nil {
		return false
	}
	var adbcErr adbc.Error
	if errors.As(err, &adbcErr) {
		if adbcErr.Code == adbc.StatusUnauthenticated || adbcErr.Code == adbc.StatusUnauthorized {
			return true
		}
	}
	// Fall back to matching the message in case the typed error is not
	// propagated through the wrapping chain.
	msg := err.Error()
	return strings.Contains(msg, "Unauthenticated") || strings.Contains(msg, "Invalid credentials")
}

// reinitADBC closes and re-opens the ADBC connection so that the next query
// performs a fresh authentication handshake. The stale argument is the
// connection the caller observed failing; if another goroutine has already
// replaced it, this is a no-op so the connection is only re-opened once.
func (c *SpiceClient) reinitADBC(stale *ADBCClient) error {
	c.adbcMu.Lock()
	defer c.adbcMu.Unlock()

	// Another caller may have already re-opened the connection we observed as
	// stale; if so, reuse theirs rather than churning the connection again.
	if c.adbcClient != stale {
		return nil
	}

	_ = c.closeADBC()
	c.adbcClient = nil
	return c.initADBC()
}

// QueryWithParams is deprecated. Use SqlWithParams instead.
// Kept for backward compatibility with v7.
func (c *SpiceClient) QueryWithParams(ctx context.Context, sql string, params ...any) (array.RecordReader, error) {
	return c.SqlWithParams(ctx, sql, params...)
}

// queryADBCWithParams executes a parameterized query using ADBC with prepare/execute pattern
func (c *SpiceClient) queryADBCWithParams(ctx context.Context, sql string, params ...any) (array.RecordReader, error) {
	if c.adbcClient == nil || c.adbcClient.conn == nil {
		return nil, fmt.Errorf("ADBC connection is not initialized")
	}

	// Create a prepared statement
	stmt, err := c.adbcClient.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("error creating statement: %w", err)
	}
	defer func() {
		if closeErr := stmt.Close(); closeErr != nil {
			// Log error but don't fail the function since we're in a defer
			log.Printf("warning: failed to close ADBC statement: %v", closeErr)
		}
	}()

	// Set the query
	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, fmt.Errorf("error setting SQL query: %w", err)
	}

	// Always call Prepare() for ADBC connections
	if err := stmt.Prepare(ctx); err != nil {
		return nil, fmt.Errorf("error preparing statement: %w", err)
	}

	// Bind parameters if provided
	if len(params) > 0 {
		if err := c.bindParameters(stmt, params...); err != nil {
			return nil, fmt.Errorf("error binding parameters: %w", err)
		}
	}

	// Execute the query
	rdr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	return rdr, nil
}

// bindParameters binds parameters to an ADBC statement
func (c *SpiceClient) bindParameters(stmt adbc.Statement, params ...any) error {
	if len(params) == 0 {
		return nil
	}

	// Extract values and types from parameters
	values := make([]any, len(params))
	types := make([]arrow.DataType, len(params))

	for i, param := range params {
		// Check if this is a Param struct with explicit type
		if p, ok := param.(Param); ok {
			values[i] = p.Value
			if p.Type != nil {
				types[i] = p.Type
			} else {
				// Infer type from value
				dataType, err := inferArrowType(p.Value)
				if err != nil {
					return fmt.Errorf("error inferring type for parameter %d: %w", i, err)
				}
				types[i] = dataType
			}
		} else {
			// Regular value, infer type
			values[i] = param
			dataType, err := inferArrowType(param)
			if err != nil {
				return fmt.Errorf("error inferring type for parameter %d: %w", i, err)
			}
			types[i] = dataType
		}
	}

	// Build Arrow schema and record for parameters
	fields := make([]arrow.Field, len(params))
	for i := range params {
		fields[i] = arrow.Field{
			Name: fmt.Sprintf("$%d", i+1),
			Type: types[i],
		}
	}

	schema := arrow.NewSchema(fields, nil)

	// Create a record builder using the reusable allocator
	bldr := array.NewRecordBuilder(c.adbcClient.mem, schema)
	defer bldr.Release()

	// Add values to the builders
	for i, value := range values {
		if err := appendValueToBuilder(bldr.Field(i), value); err != nil {
			return fmt.Errorf("error appending parameter %d: %w", i, err)
		}
	}

	// Build the record
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Bind the record to the statement
	if err := stmt.Bind(context.Background(), rec); err != nil {
		return fmt.Errorf("error binding record: %w", err)
	}

	return nil
}

// inferArrowType infers the Arrow data type from a Go value
func inferArrowType(val any) (arrow.DataType, error) {
	switch v := val.(type) {
	// Integer types
	case int8:
		return arrow.PrimitiveTypes.Int8, nil
	case int16:
		return arrow.PrimitiveTypes.Int16, nil
	case int32:
		return arrow.PrimitiveTypes.Int32, nil
	case int64:
		return arrow.PrimitiveTypes.Int64, nil
	case int:
		// int is platform-dependent, use int64 for safety
		return arrow.PrimitiveTypes.Int64, nil
	case uint8:
		return arrow.PrimitiveTypes.Uint8, nil
	case uint16:
		return arrow.PrimitiveTypes.Uint16, nil
	case uint32:
		return arrow.PrimitiveTypes.Uint32, nil
	case uint64:
		return arrow.PrimitiveTypes.Uint64, nil
	case uint:
		// uint is platform-dependent, use uint64 for safety
		return arrow.PrimitiveTypes.Uint64, nil

	// Floating point types
	case float32:
		return arrow.PrimitiveTypes.Float32, nil
	case float64:
		return arrow.PrimitiveTypes.Float64, nil

	// String and binary types
	case string:
		return arrow.BinaryTypes.String, nil
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case []byte:
		return arrow.BinaryTypes.Binary, nil

	// Temporal types
	case arrow.Date32:
		return arrow.PrimitiveTypes.Date32, nil
	case arrow.Date64:
		return arrow.PrimitiveTypes.Date64, nil
	case arrow.Time32:
		// Default to milliseconds if not specified
		return arrow.FixedWidthTypes.Time32ms, nil
	case arrow.Time64:
		// Default to microseconds if not specified
		return arrow.FixedWidthTypes.Time64us, nil
	case arrow.Timestamp:
		// Default to microseconds with UTC if not specified
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case arrow.Duration:
		// Default to microseconds if not specified
		return arrow.FixedWidthTypes.Duration_us, nil

	// Interval types
	case arrow.MonthInterval:
		return arrow.FixedWidthTypes.MonthInterval, nil
	case arrow.DayTimeInterval:
		return arrow.FixedWidthTypes.DayTimeInterval, nil
	case arrow.MonthDayNanoInterval:
		return arrow.FixedWidthTypes.MonthDayNanoInterval, nil

	// Fixed-size types
	case [16]byte:
		// Decimal128 - default precision/scale (38, 10)
		return &arrow.Decimal128Type{Precision: 38, Scale: 10}, nil
	case [32]byte:
		// Decimal256 - default precision/scale (76, 10)
		return &arrow.Decimal256Type{Precision: 76, Scale: 10}, nil

	case nil:
		return arrow.Null, nil
	default:
		return nil, fmt.Errorf("unsupported parameter type: %T (use NewTypedParam for explicit type control)", v)
	}
}

// appendValueToBuilder appends a value to an Arrow array builder
func appendValueToBuilder(builder array.Builder, val any) error {
	if val == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	// Integer builders
	case *array.Int8Builder:
		if v, ok := val.(int8); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to int8", val)
		}
	case *array.Int16Builder:
		if v, ok := val.(int16); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to int16", val)
		}
	case *array.Int32Builder:
		if v, ok := val.(int32); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to int32", val)
		}
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int:
			b.Append(int64(v))
		default:
			return fmt.Errorf("cannot convert %T to int64", val)
		}
	case *array.Uint8Builder:
		if v, ok := val.(uint8); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to uint8", val)
		}
	case *array.Uint16Builder:
		if v, ok := val.(uint16); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to uint16", val)
		}
	case *array.Uint32Builder:
		if v, ok := val.(uint32); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to uint32", val)
		}
	case *array.Uint64Builder:
		switch v := val.(type) {
		case uint64:
			b.Append(v)
		case uint:
			b.Append(uint64(v))
		default:
			return fmt.Errorf("cannot convert %T to uint64", val)
		}

	// Floating point builders
	case *array.Float16Builder:
		if v, ok := val.(uint16); ok {
			b.Append(float16.New(float32(v)))
		} else {
			return fmt.Errorf("cannot convert %T to float16", val)
		}
	case *array.Float32Builder:
		if v, ok := val.(float32); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to float32", val)
		}
	case *array.Float64Builder:
		if v, ok := val.(float64); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to float64", val)
		}

	// String and binary builders
	case *array.StringBuilder:
		if v, ok := val.(string); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to string", val)
		}
	case *array.LargeStringBuilder:
		if v, ok := val.(string); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to large string", val)
		}
	case *array.BooleanBuilder:
		if v, ok := val.(bool); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to bool", val)
		}
	case *array.BinaryBuilder:
		if v, ok := val.([]byte); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to []byte", val)
		}
	case *array.FixedSizeBinaryBuilder:
		if v, ok := val.([]byte); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to fixed size binary", val)
		}

	// Temporal builders
	case *array.Date32Builder:
		if v, ok := val.(arrow.Date32); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to arrow.Date32", val)
		}
	case *array.Date64Builder:
		if v, ok := val.(arrow.Date64); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to arrow.Date64", val)
		}
	case *array.Time32Builder:
		if v, ok := val.(arrow.Time32); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to arrow.Time32", val)
		}
	case *array.Time64Builder:
		if v, ok := val.(arrow.Time64); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to arrow.Time64", val)
		}
	case *array.TimestampBuilder:
		if v, ok := val.(arrow.Timestamp); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to arrow.Timestamp", val)
		}
	case *array.DurationBuilder:
		if v, ok := val.(arrow.Duration); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to arrow.Duration", val)
		}

	// Interval builders
	case *array.MonthIntervalBuilder:
		if v, ok := val.(arrow.MonthInterval); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to arrow.MonthInterval", val)
		}
	case *array.DayTimeIntervalBuilder:
		if v, ok := val.(arrow.DayTimeInterval); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to arrow.DayTimeInterval", val)
		}
	case *array.MonthDayNanoIntervalBuilder:
		if v, ok := val.(arrow.MonthDayNanoInterval); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to arrow.MonthDayNanoInterval", val)
		}

	// Decimal builders
	case *array.Decimal128Builder:
		if v, ok := val.([16]byte); ok {
			// Convert byte array to decimal128.Num
			// Interpret as big-endian signed integer
			hi := int64(uint64(v[0])<<56 | uint64(v[1])<<48 | uint64(v[2])<<40 | uint64(v[3])<<32 |
				uint64(v[4])<<24 | uint64(v[5])<<16 | uint64(v[6])<<8 | uint64(v[7]))
			lo := uint64(v[8])<<56 | uint64(v[9])<<48 | uint64(v[10])<<40 | uint64(v[11])<<32 |
				uint64(v[12])<<24 | uint64(v[13])<<16 | uint64(v[14])<<8 | uint64(v[15])
			dec := decimal128.New(hi, lo)
			b.Append(dec)
		} else {
			return fmt.Errorf("cannot convert %T to decimal128", val)
		}
	case *array.Decimal256Builder:
		if v, ok := val.([32]byte); ok {
			// Convert byte array to decimal256.Num
			// Extract 4 64-bit little-endian values
			w0 := uint64(v[0]) | uint64(v[1])<<8 | uint64(v[2])<<16 | uint64(v[3])<<24 |
				uint64(v[4])<<32 | uint64(v[5])<<40 | uint64(v[6])<<48 | uint64(v[7])<<56
			w1 := uint64(v[8]) | uint64(v[9])<<8 | uint64(v[10])<<16 | uint64(v[11])<<24 |
				uint64(v[12])<<32 | uint64(v[13])<<40 | uint64(v[14])<<48 | uint64(v[15])<<56
			w2 := uint64(v[16]) | uint64(v[17])<<8 | uint64(v[18])<<16 | uint64(v[19])<<24 |
				uint64(v[20])<<32 | uint64(v[21])<<40 | uint64(v[22])<<48 | uint64(v[23])<<56
			w3 := uint64(v[24]) | uint64(v[25])<<8 | uint64(v[26])<<16 | uint64(v[27])<<24 |
				uint64(v[28])<<32 | uint64(v[29])<<40 | uint64(v[30])<<48 | uint64(v[31])<<56
			dec := decimal256.New(w0, w1, w2, w3)
			b.Append(dec)
		} else {
			return fmt.Errorf("cannot convert %T to decimal256", val)
		}

	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}
