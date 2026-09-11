package gospice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Flight DoAction action types for async queries, served by Spice.ai OSS v2.0+
// when running in distributed/scheduler mode.
const (
	actionSubmitAsyncQuery    = "SubmitAsyncQuery"
	actionGetAsyncQueryStatus = "GetAsyncQueryStatus"
	actionGetAsyncQueryResult = "GetAsyncQueryResult"
	actionCancelAsyncQuery    = "CancelAsyncQuery"
)

// asyncPollInterval is how often Wait polls the runtime for status changes.
const asyncPollInterval = 500 * time.Millisecond

// QueryStatus is the lifecycle status of an async query as reported by the Spice runtime.
type QueryStatus string

const (
	QueryStatusPending   QueryStatus = "PENDING"
	QueryStatusRunning   QueryStatus = "RUNNING"
	QueryStatusSucceeded QueryStatus = "SUCCEEDED"
	QueryStatusFailed    QueryStatus = "FAILED"
	QueryStatusCancelled QueryStatus = "CANCELLED"
	QueryStatusClosed    QueryStatus = "CLOSED"
)

// IsTerminal reports whether the status is terminal (the query will not transition further).
func (s QueryStatus) IsTerminal() bool {
	switch s {
	case QueryStatusSucceeded, QueryStatusFailed, QueryStatusCancelled, QueryStatusClosed:
		return true
	default:
		return false
	}
}

// --- Flight action wire types (mirror crates/runtime/src/flight/async_actions.rs) ---

type submitAsyncQueryRequest struct {
	Sql            string  `json:"sql"`
	Parameters     any     `json:"parameters,omitempty"`
	TimeoutSeconds *uint64 `json:"timeout_seconds,omitempty"`
	MaximumSize    *uint64 `json:"maximum_size,omitempty"`
}

type submitAsyncQueryResponse struct {
	QueryID string      `json:"query_id"`
	Status  QueryStatus `json:"status"`
}

type getAsyncQueryStatusRequest struct {
	QueryID string `json:"query_id"`
}

type asyncQueryError struct {
	ErrorCode json.RawMessage `json:"error_code"`
	Message   string          `json:"message"`
}

func (e *asyncQueryError) String() string {
	code := strings.Trim(string(e.ErrorCode), `"`)
	if code == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", code, e.Message)
}

type asyncQueryResultMetadata struct {
	TotalRowCount   int `json:"total_row_count"`
	TotalChunkCount int `json:"total_chunk_count"`
}

type getAsyncQueryStatusResponse struct {
	QueryID string                    `json:"query_id"`
	Status  QueryStatus               `json:"status"`
	Error   *asyncQueryError          `json:"error,omitempty"`
	Result  *asyncQueryResultMetadata `json:"result,omitempty"`
}

type getAsyncQueryResultRequest struct {
	QueryID    string `json:"query_id"`
	ChunkIndex int    `json:"chunk_index"`
}

type cancelAsyncQueryRequest struct {
	QueryID string `json:"query_id"`
}

type cancelAsyncQueryResponse struct {
	QueryID   string      `json:"query_id"`
	Cancelled bool        `json:"cancelled"`
	Status    QueryStatus `json:"status"`
}

// AsyncQuery is a handle to a query submitted for asynchronous execution via
// SpiceClient.Query or SpiceClient.QueryWithParams. Async queries require the
// Spice runtime to be running in distributed/scheduler mode.
//
// A handle is not safe for concurrent use by multiple goroutines.
type AsyncQuery struct {
	client  *SpiceClient
	queryID string
	status  QueryStatus
}

// ID returns the server-assigned query identifier.
func (q *AsyncQuery) ID() string { return q.queryID }

// Status performs a single poll and returns the current status of the query.
func (q *AsyncQuery) Status(ctx context.Context) (QueryStatus, error) {
	resp, err := q.client.getAsyncStatus(ctx, q.queryID)
	if err != nil {
		return "", err
	}
	q.status = resp.Status
	return resp.Status, nil
}

// Wait polls the runtime until the query reaches a terminal status
// (SUCCEEDED, FAILED, CANCELLED, or CLOSED) or ctx is cancelled.
func (q *AsyncQuery) Wait(ctx context.Context) (QueryStatus, error) {
	ticker := time.NewTicker(asyncPollInterval)
	defer ticker.Stop()

	for {
		resp, err := q.client.getAsyncStatus(ctx, q.queryID)
		if err != nil {
			return "", err
		}
		q.status = resp.Status
		if resp.Status.IsTerminal() {
			return resp.Status, nil
		}

		select {
		case <-ctx.Done():
			return q.status, ctx.Err()
		case <-ticker.C:
		}
	}
}

// Results waits for the query to complete and returns its results as an Apache
// Arrow RecordReader. The caller must Release the returned reader.
//
// If the query does not succeed, Results returns an error describing the
// terminal status (including the runtime error message when available).
func (q *AsyncQuery) Results(ctx context.Context) (array.RecordReader, error) {
	status, err := q.Wait(ctx)
	if err != nil {
		return nil, err
	}

	statusResp, err := q.client.getAsyncStatus(ctx, q.queryID)
	if err != nil {
		return nil, err
	}
	if status != QueryStatusSucceeded {
		if statusResp.Error != nil {
			return nil, fmt.Errorf("async query %s %s: %s", q.queryID, status, statusResp.Error.String())
		}
		return nil, fmt.Errorf("async query %s did not succeed (status: %s)", q.queryID, status)
	}

	chunkCount := 0
	if statusResp.Result != nil {
		chunkCount = statusResp.Result.TotalChunkCount
	}

	// Always fetch at least one chunk so we obtain a schema, even for empty results.
	fetchCount := chunkCount
	if fetchCount == 0 {
		fetchCount = 1
	}

	var (
		records []arrow.RecordBatch
		schema  *arrow.Schema
	)
	for i := 0; i < fetchCount; i++ {
		body, err := q.client.doAction(ctx, actionGetAsyncQueryResult,
			getAsyncQueryResultRequest{QueryID: q.queryID, ChunkIndex: i})
		if err != nil {
			// A missing chunk 0 on a genuinely empty result is not an error.
			if i == 0 && chunkCount == 0 {
				break
			}
			releaseRecords(records)
			return nil, fmt.Errorf("failed to fetch result chunk %d for query %s: %w", i, q.queryID, err)
		}

		rdr, err := ipc.NewReader(bytes.NewReader(body))
		if err != nil {
			releaseRecords(records)
			return nil, fmt.Errorf("failed to decode result chunk %d for query %s: %w", i, q.queryID, err)
		}
		if schema == nil {
			schema = rdr.Schema()
		}
		for rdr.Next() {
			rec := rdr.RecordBatch()
			rec.Retain()
			records = append(records, rec)
		}
		if err := rdr.Err(); err != nil {
			rdr.Release()
			releaseRecords(records)
			return nil, fmt.Errorf("error reading result chunk %d for query %s: %w", i, q.queryID, err)
		}
		rdr.Release()
	}

	if schema == nil {
		schema = arrow.NewSchema([]arrow.Field{}, nil)
	}

	reader, err := array.NewRecordReader(schema, records)
	// NewRecordReader retains its own references to the records; drop ours.
	releaseRecords(records)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// Cancel requests cancellation of the query. It is best-effort: a query that
// has already reached a terminal status will not be cancelled, which is not
// reported as an error. Inspect Status to observe the outcome.
func (q *AsyncQuery) Cancel(ctx context.Context) error {
	body, err := q.client.doAction(ctx, actionCancelAsyncQuery,
		cancelAsyncQueryRequest{QueryID: q.queryID})
	if err != nil {
		return err
	}
	var resp cancelAsyncQueryResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("failed to parse cancel response: %w", err)
	}
	q.status = resp.Status
	return nil
}

// --- SpiceClient async submission ---

// Query submits sql to the Spice runtime for asynchronous execution and returns
// a handle for polling status and retrieving results. Async queries require the
// runtime to be running in distributed/scheduler mode; otherwise the runtime
// returns an error indicating async queries are only available in cluster mode.
//
// Use Sql for synchronous, streaming queries.
func (c *SpiceClient) Query(ctx context.Context, sql string) (*AsyncQuery, error) {
	return c.submitAsync(ctx, sql, nil)
}

// QueryWithParams submits a parameterized query for asynchronous execution.
// Parameters are bound positionally ($1, $2, ...) and are sent as a JSON array,
// so each must be a JSON-encodable value.
//
// Use SqlWithParams for synchronous, streaming parameterized queries.
func (c *SpiceClient) QueryWithParams(ctx context.Context, sql string, params ...any) (*AsyncQuery, error) {
	var parameters any
	if len(params) > 0 {
		parameters = params
	}
	return c.submitAsync(ctx, sql, parameters)
}

func (c *SpiceClient) submitAsync(ctx context.Context, sql string, parameters any) (*AsyncQuery, error) {
	body, err := c.doAction(ctx, actionSubmitAsyncQuery,
		submitAsyncQueryRequest{Sql: sql, Parameters: parameters})
	if err != nil {
		return nil, fmt.Errorf("failed to submit async query: %w", err)
	}
	var resp submitAsyncQueryResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse submit response: %w", err)
	}
	return &AsyncQuery{client: c, queryID: resp.QueryID, status: resp.Status}, nil
}

// doAction performs a Flight DoAction with a JSON-encoded request body under the
// client's Flight session and returns the concatenated Result bodies.
func (c *SpiceClient) doAction(ctx context.Context, actionType string, request any) ([]byte, error) {
	if c.flightClient == nil {
		return nil, fmt.Errorf("flight client is not initialized")
	}
	reqBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal %s request: %w", actionType, err)
	}

	var out []byte
	err = c.withSession(ctx, func(ctx context.Context) error {
		stream, err := c.flightClient.DoAction(ctx, &flight.Action{Type: actionType, Body: reqBody})
		if err != nil {
			return err
		}

		out = out[:0]
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			out = append(out, res.Body...)
		}
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *SpiceClient) getAsyncStatus(ctx context.Context, queryID string) (*getAsyncQueryStatusResponse, error) {
	body, err := c.doAction(ctx, actionGetAsyncQueryStatus, getAsyncQueryStatusRequest{QueryID: queryID})
	if err != nil {
		return nil, err
	}
	var resp getAsyncQueryStatusResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse status response: %w", err)
	}
	return &resp, nil
}

func releaseRecords(recs []arrow.RecordBatch) {
	for _, r := range recs {
		r.Release()
	}
}
