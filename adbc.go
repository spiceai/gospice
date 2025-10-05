package gospice

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ADBCClient wraps ADBC database and connection for Spice.ai
type ADBCClient struct {
	db   adbc.Database
	conn adbc.Connection
}

// initADBC initializes the ADBC connection
func (c *SpiceClient) initADBC() error {
	// Create ADBC driver instance
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
		db.Close()
		return fmt.Errorf("error opening ADBC connection: %w", err)
	}

	c.adbcClient = &ADBCClient{
		db:   db,
		conn: conn,
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
func (c *SpiceClient) SqlWithParams(ctx context.Context, sql string, params ...interface{}) (array.RecordReader, error) {
	if c.adbcClient == nil {
		// Try lazy initialization
		if err := c.initADBC(); err != nil {
			return nil, fmt.Errorf("ADBC client is not initialized and failed to initialize: %w", err)
		}
	}

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

// QueryWithParams is deprecated. Use SqlWithParams instead.
// Kept for backward compatibility with v7.
func (c *SpiceClient) QueryWithParams(ctx context.Context, sql string, params ...interface{}) (array.RecordReader, error) {
	return c.SqlWithParams(ctx, sql, params...)
}

// queryADBCWithParams executes a parameterized query using ADBC
func (c *SpiceClient) queryADBCWithParams(ctx context.Context, sql string, params ...interface{}) (array.RecordReader, error) {
	if c.adbcClient == nil || c.adbcClient.conn == nil {
		return nil, fmt.Errorf("ADBC connection is not initialized")
	}

	// Create a prepared statement
	stmt, err := c.adbcClient.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("error creating statement: %w", err)
	}
	defer stmt.Close()

	// Set the query
	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, fmt.Errorf("error setting SQL query: %w", err)
	}

	// If we have parameters, prepare the statement and bind them
	if len(params) > 0 {
		// Prepare the statement before binding parameters
		if err := stmt.Prepare(ctx); err != nil {
			return nil, fmt.Errorf("error preparing statement: %w", err)
		}

		// Bind parameters
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
func (c *SpiceClient) bindParameters(stmt adbc.Statement, params ...interface{}) error {
	if len(params) == 0 {
		return nil
	}

	// Build Arrow schema and record for parameters
	fields := make([]arrow.Field, len(params))
	for i, param := range params {
		dataType, err := inferArrowType(param)
		if err != nil {
			return fmt.Errorf("error inferring type for parameter %d: %w", i, err)
		}
		fields[i] = arrow.Field{
			Name: fmt.Sprintf("$%d", i+1),
			Type: dataType,
		}
	}

	schema := arrow.NewSchema(fields, nil)

	// Create a record builder
	mem := memory.NewGoAllocator()
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	// Add values to the builders
	for i, param := range params {
		if err := appendValueToBuilder(bldr.Field(i), param); err != nil {
			return fmt.Errorf("error appending parameter %d: %w", i, err)
		}
	}

	// Build the record
	rec := bldr.NewRecord()
	defer rec.Release()

	// Bind the record to the statement
	if err := stmt.Bind(context.Background(), rec); err != nil {
		return fmt.Errorf("error binding record: %w", err)
	}

	return nil
}

// inferArrowType infers the Arrow data type from a Go value
func inferArrowType(val interface{}) (arrow.DataType, error) {
	switch val.(type) {
	case int, int8, int16, int32:
		return arrow.PrimitiveTypes.Int32, nil
	case int64:
		return arrow.PrimitiveTypes.Int64, nil
	case uint, uint8, uint16, uint32:
		return arrow.PrimitiveTypes.Uint32, nil
	case uint64:
		return arrow.PrimitiveTypes.Uint64, nil
	case float32:
		return arrow.PrimitiveTypes.Float32, nil
	case float64:
		return arrow.PrimitiveTypes.Float64, nil
	case string:
		return arrow.BinaryTypes.String, nil
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case []byte:
		return arrow.BinaryTypes.Binary, nil
	case nil:
		return arrow.Null, nil
	default:
		return nil, fmt.Errorf("unsupported parameter type: %T", val)
	}
}

// appendValueToBuilder appends a value to an Arrow array builder
func appendValueToBuilder(builder array.Builder, val interface{}) error {
	if val == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.Int32Builder:
		switch v := val.(type) {
		case int:
			b.Append(int32(v))
		case int8:
			b.Append(int32(v))
		case int16:
			b.Append(int32(v))
		case int32:
			b.Append(v)
		default:
			return fmt.Errorf("cannot convert %T to int32", val)
		}
	case *array.Int64Builder:
		if v, ok := val.(int64); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to int64", val)
		}
	case *array.Uint32Builder:
		switch v := val.(type) {
		case uint:
			b.Append(uint32(v))
		case uint8:
			b.Append(uint32(v))
		case uint16:
			b.Append(uint32(v))
		case uint32:
			b.Append(v)
		default:
			return fmt.Errorf("cannot convert %T to uint32", val)
		}
	case *array.Uint64Builder:
		if v, ok := val.(uint64); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to uint64", val)
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
	case *array.StringBuilder:
		if v, ok := val.(string); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %T to string", val)
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
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}
