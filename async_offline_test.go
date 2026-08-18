package gospice

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// asyncActionHandler answers a single Flight DoAction call for one action type.
type asyncActionHandler func(requestBody []byte) (responseBody []byte, err error)

// asyncTestServer is a minimal in-process Flight server that only implements
// DoAction, so the async query API (SubmitAsyncQuery/GetAsyncQueryStatus/
// GetAsyncQueryResult/CancelAsyncQuery) can be exercised without a real spiced.
type asyncTestServer struct {
	flight.BaseFlightServer

	mu       sync.Mutex
	handlers map[string]asyncActionHandler
	calls    map[string]int
}

func newAsyncTestServer() *asyncTestServer {
	return &asyncTestServer{
		handlers: make(map[string]asyncActionHandler),
		calls:    make(map[string]int),
	}
}

func (s *asyncTestServer) on(actionType string, h asyncActionHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[actionType] = h
}

func (s *asyncTestServer) callCount(actionType string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[actionType]
}

func (s *asyncTestServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	s.mu.Lock()
	s.calls[action.Type]++
	h, ok := s.handlers[action.Type]
	s.mu.Unlock()

	if !ok {
		return status.Errorf(codes.Unimplemented, "asyncTestServer: no handler registered for action %s", action.Type)
	}
	body, err := h(action.Body)
	if err != nil {
		return err
	}
	return stream.Send(&flight.Result{Body: body})
}

// startAsyncTestServer starts srv on an ephemeral local port and returns its
// grpc:// address, ready to pass to WithFlightAddress.
func startAsyncTestServer(t *testing.T, srv *asyncTestServer) string {
	t.Helper()

	fs := flight.NewFlightServer()
	if err := fs.Init("127.0.0.1:0"); err != nil {
		t.Fatalf("error starting test flight server: %v", err)
	}
	fs.RegisterFlightService(srv)

	go func() {
		_ = fs.Serve()
	}()
	t.Cleanup(fs.Shutdown)

	return "grpc://" + fs.Addr().String()
}

func newAsyncTestClient(t *testing.T, addr string) *SpiceClient {
	t.Helper()

	spice := NewSpiceClient()
	if err := spice.Init(WithFlightAddress(addr)); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}
	t.Cleanup(func() { _ = spice.Close() })
	return spice
}

// buildIPCChunk encodes a single-column, three-row Int64 record batch as an
// Arrow IPC stream, mirroring what GetAsyncQueryResult returns for one chunk.
func buildIPCChunk(t *testing.T) []byte {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)

	bldr := array.NewInt64Builder(memory.DefaultAllocator)
	defer bldr.Release()
	bldr.AppendValues([]int64{1, 2, 3}, nil)
	col := bldr.NewInt64Array()
	defer col.Release()

	rec := array.NewRecord(schema, []arrow.Array{col}, 3)
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(rec); err != nil {
		t.Fatalf("error writing IPC record: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("error closing IPC writer: %v", err)
	}
	return buf.Bytes()
}

func TestAsyncQuerySubmitAndStatus(t *testing.T) {
	srv := newAsyncTestServer()
	srv.on(actionSubmitAsyncQuery, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-1","status":"PENDING"}`), nil
	})
	srv.on(actionGetAsyncQueryStatus, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-1","status":"RUNNING"}`), nil
	})

	spice := newAsyncTestClient(t, startAsyncTestServer(t, srv))

	q, err := spice.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("error submitting async query: %v", err)
	}
	if q.ID() != "q-1" {
		t.Errorf("ID() = %q, want %q", q.ID(), "q-1")
	}
	if q.status != QueryStatusPending {
		t.Errorf("initial status = %q, want %q", q.status, QueryStatusPending)
	}

	got, err := q.Status(context.Background())
	if err != nil {
		t.Fatalf("error polling status: %v", err)
	}
	if got != QueryStatusRunning {
		t.Errorf("Status() = %q, want %q", got, QueryStatusRunning)
	}
}

func TestAsyncQueryWaitPollsUntilTerminal(t *testing.T) {
	srv := newAsyncTestServer()
	srv.on(actionSubmitAsyncQuery, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-2","status":"PENDING"}`), nil
	})

	var polls int
	srv.on(actionGetAsyncQueryStatus, func(body []byte) ([]byte, error) {
		polls++
		if polls < 3 {
			return []byte(`{"query_id":"q-2","status":"RUNNING"}`), nil
		}
		return []byte(`{"query_id":"q-2","status":"SUCCEEDED","result":{"total_row_count":0,"total_chunk_count":0}}`), nil
	})

	spice := newAsyncTestClient(t, startAsyncTestServer(t, srv))

	q, err := spice.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("error submitting async query: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := q.Wait(ctx)
	if err != nil {
		t.Fatalf("error waiting for query: %v", err)
	}
	if got != QueryStatusSucceeded {
		t.Errorf("Wait() = %q, want %q", got, QueryStatusSucceeded)
	}
	if polls < 3 {
		t.Errorf("expected Wait to poll until the 3rd response, only polled %d times", polls)
	}
}

func TestAsyncQueryResultsEmpty(t *testing.T) {
	srv := newAsyncTestServer()
	srv.on(actionSubmitAsyncQuery, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-3","status":"PENDING"}`), nil
	})
	srv.on(actionGetAsyncQueryStatus, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-3","status":"SUCCEEDED","result":{"total_row_count":0,"total_chunk_count":0}}`), nil
	})
	srv.on(actionGetAsyncQueryResult, func(body []byte) ([]byte, error) {
		return nil, status.Errorf(codes.NotFound, "no chunks for an empty result")
	})

	spice := newAsyncTestClient(t, startAsyncTestServer(t, srv))

	q, err := spice.Query(context.Background(), "SELECT 1 WHERE false")
	if err != nil {
		t.Fatalf("error submitting async query: %v", err)
	}

	reader, err := q.Results(context.Background())
	if err != nil {
		t.Fatalf("error fetching results: %v", err)
	}
	defer reader.Release()

	if reader.Next() {
		t.Error("expected no records for an empty result")
	}
}

func TestAsyncQueryResultsWithData(t *testing.T) {
	chunk := buildIPCChunk(t)

	srv := newAsyncTestServer()
	srv.on(actionSubmitAsyncQuery, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-4","status":"PENDING"}`), nil
	})
	srv.on(actionGetAsyncQueryStatus, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-4","status":"SUCCEEDED","result":{"total_row_count":3,"total_chunk_count":1}}`), nil
	})
	srv.on(actionGetAsyncQueryResult, func(body []byte) ([]byte, error) {
		return chunk, nil
	})

	spice := newAsyncTestClient(t, startAsyncTestServer(t, srv))

	q, err := spice.Query(context.Background(), "SELECT n FROM t")
	if err != nil {
		t.Fatalf("error submitting async query: %v", err)
	}

	reader, err := q.Results(context.Background())
	if err != nil {
		t.Fatalf("error fetching results: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("expected one record batch, got none")
	}
	rec := reader.RecordBatch()
	if rec.NumRows() != 3 {
		t.Errorf("NumRows() = %d, want 3", rec.NumRows())
	}
	col, ok := rec.Column(0).(*array.Int64)
	if !ok {
		t.Fatalf("column 0 is %T, want *array.Int64", rec.Column(0))
	}
	want := []int64{1, 2, 3}
	for i, w := range want {
		if got := col.Value(i); got != w {
			t.Errorf("row %d = %d, want %d", i, got, w)
		}
	}
	if reader.Next() {
		t.Error("expected exactly one record batch")
	}
}

func TestAsyncQueryResultsFailure(t *testing.T) {
	srv := newAsyncTestServer()
	srv.on(actionSubmitAsyncQuery, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-5","status":"PENDING"}`), nil
	})
	srv.on(actionGetAsyncQueryStatus, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-5","status":"FAILED","error":{"error_code":"QueryExecutionError","message":"boom"}}`), nil
	})

	spice := newAsyncTestClient(t, startAsyncTestServer(t, srv))

	q, err := spice.Query(context.Background(), "SELECT 1/0")
	if err != nil {
		t.Fatalf("error submitting async query: %v", err)
	}

	_, err = q.Results(context.Background())
	if err == nil {
		t.Fatal("expected an error for a failed query, got nil")
	}
	if got := err.Error(); !bytes.Contains([]byte(got), []byte("boom")) {
		t.Errorf("error %q does not mention the runtime's failure message", got)
	}
}

func TestAsyncQueryCancel(t *testing.T) {
	srv := newAsyncTestServer()
	srv.on(actionSubmitAsyncQuery, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-6","status":"RUNNING"}`), nil
	})
	srv.on(actionCancelAsyncQuery, func(body []byte) ([]byte, error) {
		return []byte(`{"query_id":"q-6","cancelled":true,"status":"CANCELLED"}`), nil
	})

	spice := newAsyncTestClient(t, startAsyncTestServer(t, srv))

	q, err := spice.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("error submitting async query: %v", err)
	}

	if err := q.Cancel(context.Background()); err != nil {
		t.Fatalf("error cancelling query: %v", err)
	}
	if q.status != QueryStatusCancelled {
		t.Errorf("status after Cancel() = %q, want %q", q.status, QueryStatusCancelled)
	}
	if got := srv.callCount(actionCancelAsyncQuery); got != 1 {
		t.Errorf("CancelAsyncQuery action called %d times, want 1", got)
	}
}

func TestAsyncQueryWithParams(t *testing.T) {
	var submittedBody []byte
	srv := newAsyncTestServer()
	srv.on(actionSubmitAsyncQuery, func(body []byte) ([]byte, error) {
		submittedBody = body
		return []byte(`{"query_id":"q-7","status":"PENDING"}`), nil
	})

	spice := newAsyncTestClient(t, startAsyncTestServer(t, srv))

	_, err := spice.QueryWithParams(context.Background(), "SELECT * FROM t WHERE id = $1", 42)
	if err != nil {
		t.Fatalf("error submitting parameterized async query: %v", err)
	}

	want := `"parameters":[42]`
	if !bytes.Contains(submittedBody, []byte(want)) {
		t.Errorf("submitted request body %s does not contain %s", submittedBody, want)
	}
	wantSQL := fmt.Sprintf(`"sql":%q`, "SELECT * FROM t WHERE id = $1")
	if !bytes.Contains(submittedBody, []byte(wantSQL)) {
		t.Errorf("submitted request body %s does not contain %s", submittedBody, wantSQL)
	}
}
