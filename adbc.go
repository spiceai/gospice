package gospice

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"

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

	// mu guards the lease bookkeeping below.
	//
	// A connection cannot simply be closed when re-authentication replaces it:
	// other goroutines may still be executing against it, and a RecordReader
	// returned by SqlWithParams streams from it long after that call returned.
	// Closing underneath either one turns another caller's in-flight query into
	// a use-after-close. Instead a replaced connection is *retired* and closed
	// only once the last lease on it is dropped.
	mu      sync.Mutex
	leases  int
	retired bool
	closed  bool
}

// acquire records one user of the connection. Every acquire must be paired with
// exactly one release, and the lease that travels with a returned RecordReader
// is dropped when that reader is released.
func (a *ADBCClient) acquire() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.leases++
}

// release drops one lease, closing the connection if it has been retired and
// this was its last user.
func (a *ADBCClient) release() {
	a.mu.Lock()
	a.leases--
	shouldClose := a.retired && a.leases <= 0 && !a.closed
	if shouldClose {
		a.closed = true
	}
	a.mu.Unlock()

	if shouldClose {
		if err := a.closeNow(); err != nil {
			log.Printf("warning: failed to close retired ADBC connection: %v", err)
		}
	}
}

// retire marks the connection as replaced. It closes immediately when nothing
// holds a lease, and otherwise leaves the close to the last release.
func (a *ADBCClient) retire() error {
	a.mu.Lock()
	a.retired = true
	shouldClose := a.leases <= 0 && !a.closed
	if shouldClose {
		a.closed = true
	}
	a.mu.Unlock()

	if shouldClose {
		return a.closeNow()
	}
	return nil
}

// closeNow closes the connection and database. Callers must have claimed the
// close by setting closed under mu, so this runs at most once.
func (a *ADBCClient) closeNow() error {
	var errs []error
	if a.conn != nil {
		if err := a.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if a.db != nil {
		if err := a.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("error closing ADBC client: %v", errs)
	}
	return nil
}

// leasedRecordReader keeps the ADBC connection a reader streams from alive for
// as long as the reader is. It mirrors the wrapped reader's own retain/release
// refcount and drops the connection lease when that count reaches zero.
type leasedRecordReader struct {
	array.RecordReader
	client *ADBCClient
	refs   atomic.Int64
}

func newLeasedRecordReader(rdr array.RecordReader, client *ADBCClient) *leasedRecordReader {
	r := &leasedRecordReader{RecordReader: rdr, client: client}
	r.refs.Store(1)
	return r
}

func (r *leasedRecordReader) Retain() {
	r.refs.Add(1)
	r.RecordReader.Retain()
}

func (r *leasedRecordReader) Release() {
	r.RecordReader.Release()
	if r.refs.Add(-1) == 0 {
		r.client.release()
	}
}

// adbcOptions builds the FlightSQL driver options for the current client
// configuration: endpoint URI, credentials, TLS material and user agent.
func (c *SpiceClient) adbcOptions() (map[string]string, error) {
	// A custom CA or a client certificate only means anything over TLS, so
	// configuring either forces the TLS scheme rather than letting the
	// :443/spiceai.io heuristic below decide.
	tlsConfigured := c.tlsRootCertFile != "" || (c.tlsClientCertFile != "" && c.tlsClientKeyFile != "")

	// Format the URI correctly for ADBC FlightSQL driver
	uri := c.flightAddress
	// If it doesn't start with grpc:// or grpc+tls://, add the appropriate scheme
	if !strings.HasPrefix(uri, "grpc://") && !strings.HasPrefix(uri, "grpc+tls://") {
		// For cloud addresses (with port 443 or containing spiceai.io), use grpc+tls
		if tlsConfigured || strings.Contains(uri, ":443") || strings.Contains(uri, "spiceai.io") {
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

	// Carry the same TLS material the Flight and HTTP clients use. Without it
	// SqlWithParams is the odd one out: Init and every HTTP method succeed
	// against a private-CA or mTLS runtime while this connection fails
	// verification, or connects unauthenticated, depending on the server.
	if c.tlsRootCertFile != "" {
		caPem, err := os.ReadFile(c.tlsRootCertFile)
		if err != nil {
			return nil, fmt.Errorf("error reading TLS root certificate '%s': %w", c.tlsRootCertFile, err)
		}
		options[flightsql.OptionSSLRootCerts] = string(caPem)
	}
	if c.tlsClientCertFile != "" && c.tlsClientKeyFile != "" {
		certPem, err := os.ReadFile(c.tlsClientCertFile)
		if err != nil {
			return nil, fmt.Errorf("error reading TLS client certificate '%s': %w", c.tlsClientCertFile, err)
		}
		keyPem, err := os.ReadFile(c.tlsClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("error reading TLS client key '%s': %w", c.tlsClientKeyFile, err)
		}
		options[flightsql.OptionMTLSCertChain] = string(certPem)
		options[flightsql.OptionMTLSPrivateKey] = string(keyPem)
	}

	// Add user agent header
	if c.userAgent != "" {
		options["adbc.flight.sql.rpc.call_header.user-agent"] = c.userAgent
	}

	return options, nil
}

// initADBC initializes the ADBC connection
func (c *SpiceClient) initADBC() error {
	// Create reusable memory allocator
	mem := memory.NewGoAllocator()
	driver := flightsql.NewDriver(mem)

	options, err := c.adbcOptions()
	if err != nil {
		return err
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

// closeADBC retires the ADBC connection and database.
//
// The connection closes as soon as nothing is using it: immediately when no
// query is in flight and no returned RecordReader is still streaming from it,
// and otherwise when the last of those releases its lease.
func (c *SpiceClient) closeADBC() error {
	c.adbcMu.Lock()
	client := c.adbcClient
	c.adbcClient = nil
	c.adbcMu.Unlock()

	if client == nil {
		return nil
	}
	return client.retire()
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
	// Take a lease on the ADBC connection so a concurrent re-authentication can
	// replace it without closing it underneath this query.
	used, err := c.acquireADBC()
	if err != nil {
		return nil, fmt.Errorf("ADBC client is not initialized and failed to initialize: %w", err)
	}

	rdr, err := c.execADBCWithBackoff(ctx, used, sql, params...)
	if err != nil && isADBCAuthError(err) {
		// The ADBC connection authenticates only once, when it is opened: a
		// Basic-auth handshake yields a server-side session token that is then
		// reused for every prepared statement on that connection. That session
		// can be invalidated server-side (e.g. expired after a period of
		// inactivity), after which the cached token is rejected on every
		// subsequent request and the connection cannot recover on its own.
		// Re-open the connection to perform a fresh handshake, then retry once.
		fresh, reinitErr := c.reinitADBC(used)
		// Done with the stale connection either way; it closes once every other
		// user has also released it.
		used.release()
		if reinitErr != nil {
			return nil, fmt.Errorf("ADBC re-authentication failed: %w (original error: %v)", reinitErr, err)
		}
		// reinitADBC hands back a connection with a lease already taken for us.
		used = fresh
		rdr, err = c.execADBCWithBackoff(ctx, used, sql, params...)
	}
	if err != nil {
		used.release()
		return nil, err
	}

	// The reader goes on streaming from the connection after this returns, so
	// the lease travels with it and is dropped when it is released.
	return newLeasedRecordReader(rdr, used), nil
}

// acquireADBC returns the current ADBC connection with a lease taken on it,
// lazily initializing it if necessary. The read, initialization and acquire run
// under adbcMu, so the returned connection cannot be retired and closed between
// being observed here and being used by the caller. The caller must release it.
func (c *SpiceClient) acquireADBC() (*ADBCClient, error) {
	c.adbcMu.Lock()
	defer c.adbcMu.Unlock()

	if c.adbcClient == nil {
		if err := c.initADBC(); err != nil {
			return nil, err
		}
	}
	c.adbcClient.acquire()
	return c.adbcClient, nil
}

// execADBCWithBackoff runs a parameterized ADBC query, retrying transient
// (e.g. Unavailable / Internal) failures with the client's backoff policy.
// Authentication failures are returned as-is (not retried here) so the caller
// can re-establish the connection before retrying.
func (c *SpiceClient) execADBCWithBackoff(ctx context.Context, client *ADBCClient, sql string, params ...any) (array.RecordReader, error) {
	var rdr array.RecordReader
	err := backoff.Retry(func() error {
		var err error
		rdr, err = c.queryADBCWithParams(ctx, client, sql, params...)
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
	// Match both the value and pointer forms of adbc.Error, since the driver
	// or wrapping layers may return either.
	var adbcErr adbc.Error
	if errors.As(err, &adbcErr) && isADBCAuthStatus(adbcErr.Code) {
		return true
	}
	var adbcErrPtr *adbc.Error
	if errors.As(err, &adbcErrPtr) && adbcErrPtr != nil && isADBCAuthStatus(adbcErrPtr.Code) {
		return true
	}
	// Fall back to matching the message in case the typed error is not
	// propagated through the wrapping chain.
	msg := err.Error()
	return strings.Contains(msg, "Unauthenticated") || strings.Contains(msg, "Invalid credentials")
}

// isADBCAuthStatus reports whether an ADBC status code indicates the server
// rejected the connection's credentials or session.
//
// StatusUnauthorized is deliberately excluded: it means the credential is
// recognised but is not permitted to perform the operation, which a fresh
// handshake with the same credential cannot fix. Reconnecting on it would churn
// the connection and retire it out from under concurrent queries, only to fail
// the retry with the same error.
func isADBCAuthStatus(code adbc.Status) bool {
	return code == adbc.StatusUnauthenticated
}

// reinitADBC re-opens the ADBC connection so that the next query performs a
// fresh authentication handshake, returning the connection to use for the retry
// with a lease already taken for the caller. The stale argument is the
// connection the caller observed failing; if another goroutine has already
// replaced it with a live one, that connection is reused so the connection is
// only re-opened once.
//
// The stale connection is retired rather than closed: other goroutines may
// still be executing against it, and readers already returned to callers go on
// streaming from it. It closes once the last of them releases its lease.
func (c *SpiceClient) reinitADBC(stale *ADBCClient) (*ADBCClient, error) {
	c.adbcMu.Lock()
	defer c.adbcMu.Unlock()

	// Another caller may have already re-opened the connection we observed as
	// stale; if so, reuse theirs rather than churning the connection again.
	if c.adbcClient != stale && c.adbcClient != nil {
		c.adbcClient.acquire()
		return c.adbcClient, nil
	}

	if stale != nil {
		if retireErr := stale.retire(); retireErr != nil {
			log.Printf("warning: failed to close stale ADBC connection during re-authentication: %v", retireErr)
		}
	}
	c.adbcClient = nil
	if err := c.initADBC(); err != nil {
		return nil, err
	}
	c.adbcClient.acquire()
	return c.adbcClient, nil
}

// queryADBCWithParams executes a parameterized query using ADBC with prepare/execute pattern
func (c *SpiceClient) queryADBCWithParams(ctx context.Context, client *ADBCClient, sql string, params ...any) (array.RecordReader, error) {
	if client == nil || client.conn == nil {
		return nil, fmt.Errorf("ADBC connection is not initialized")
	}

	// Create a prepared statement
	stmt, err := client.conn.NewStatement()
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
		if err := c.bindParameters(client, stmt, params...); err != nil {
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
func (c *SpiceClient) bindParameters(client *ADBCClient, stmt adbc.Statement, params ...any) error {
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
	bldr := array.NewRecordBuilder(client.mem, schema)
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
