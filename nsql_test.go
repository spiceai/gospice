package gospice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newNsqlTestClient returns a client pointed at handler, without dialing
// Flight - Nsql only uses the HTTP control plane.
func newNsqlTestClient(t *testing.T, handler http.HandlerFunc) *SpiceClient {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	spice := NewSpiceClient()
	if err := WithHttpAddress(server.URL)(spice); err != nil {
		t.Fatalf("error setting http address: %v", err)
	}
	return spice
}

func TestNsqlRequestEncoding(t *testing.T) {
	tests := []struct {
		name string
		req  *NsqlRequest
		want map[string]any
	}{
		{
			name: "query only",
			req:  &NsqlRequest{Query: "top 5 customers by revenue"},
			want: map[string]any{"query": "top 5 customers by revenue"},
		},
		{
			name: "all options",
			req: &NsqlRequest{
				Query:             "top 5 customers by revenue",
				Model:             "nsql-model",
				Datasets:          []string{"sales"},
				SampleDataEnabled: true,
				PromptCacheKey:    "sales-dashboard",
			},
			want: map[string]any{
				"query":               "top 5 customers by revenue",
				"model":               "nsql-model",
				"datasets":            []any{"sales"},
				"sample_data_enabled": true,
				"prompt_cache_key":    "sales-dashboard",
			},
		},
		{
			// sample_data_enabled defaults to false server-side, so omitting
			// it rather than sending false keeps the body minimal.
			name: "empty datasets and false sampling omitted",
			req:  &NsqlRequest{Query: "how many orders", Datasets: []string{}},
			want: map[string]any{"query": "how many orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got map[string]any

			spice := newNsqlTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("error reading request body: %v", err)
				}
				if err := json.Unmarshal(body, &got); err != nil {
					t.Errorf("error decoding request body: %v", err)
				}
				_, _ = io.WriteString(w, `{"row_count":0,"schema":{},"data":[],"sql":"SELECT 1"}`)
			})

			if _, err := spice.Nsql(context.Background(), tt.req); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			gotJSON, _ := json.Marshal(got)
			wantJSON, _ := json.Marshal(tt.want)
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("request body = %s, want %s", gotJSON, wantJSON)
			}
		})
	}
}

// The runtime only returns the envelope carrying the generated SQL when the
// request asks for it; the default JSON representation is a bare array of
// rows. Nsql is useless without this header, so pin it.
func TestNsqlRequestHeaders(t *testing.T) {
	var gotPath, gotMethod, gotAccept string

	spice := newNsqlTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		gotAccept = r.Header.Get("Accept")
		_, _ = io.WriteString(w, `{"row_count":0,"schema":{},"data":[],"sql":"SELECT 1"}`)
	})

	if _, err := spice.Nsql(context.Background(), &NsqlRequest{Query: "how many orders"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotPath != "/v1/nsql" {
		t.Errorf("path = %q, want %q", gotPath, "/v1/nsql")
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want %q", gotMethod, http.MethodPost)
	}
	if gotAccept != nsqlJSONMediaType {
		t.Errorf("Accept = %q, want %q", gotAccept, nsqlJSONMediaType)
	}
}

func TestNsqlResponseDecoding(t *testing.T) {
	body := `{
		"row_count": 2,
		"schema": {
			"fields": [
				{"name": "customer_id", "data_type": "Utf8", "nullable": false},
				{"name": "ts", "data_type": {"Timestamp": ["Nanosecond", null]}, "nullable": true}
			]
		},
		"data": [
			{"customer_id": "12345", "ts": 1724716542},
			{"customer_id": "67890", "ts": 1724716543}
		],
		"sql": "SELECT customer_id, ts FROM sales LIMIT 2"
	}`

	spice := newNsqlTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	})

	resp, err := spice.Nsql(context.Background(), &NsqlRequest{Query: "recent customers"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.SQL != "SELECT customer_id, ts FROM sales LIMIT 2" {
		t.Errorf("SQL = %q, want the generated query", resp.SQL)
	}
	if resp.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", resp.RowCount)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("len(Data) = %d, want 2", len(resp.Data))
	}
	if resp.Data[0]["customer_id"] != "12345" {
		t.Errorf("Data[0][customer_id] = %v, want %q", resp.Data[0]["customer_id"], "12345")
	}

	if len(resp.Schema.Fields) != 2 {
		t.Fatalf("len(Schema.Fields) = %d, want 2", len(resp.Schema.Fields))
	}
	if resp.Schema.Fields[0].Name != "customer_id" {
		t.Errorf("Fields[0].Name = %q, want %q", resp.Schema.Fields[0].Name, "customer_id")
	}
	// A simple Arrow type encodes as a quoted string, a parameterized one as
	// an object - which is why DataType stays raw JSON.
	if got := string(resp.Schema.Fields[0].DataType); got != `"Utf8"` {
		t.Errorf("Fields[0].DataType = %s, want %s", got, `"Utf8"`)
	}
	if got := string(resp.Schema.Fields[1].DataType); got != `{"Timestamp": ["Nanosecond", null]}` {
		t.Errorf("Fields[1].DataType = %s, want the Timestamp object", got)
	}
	if resp.Schema.Fields[1].Nullable != true {
		t.Error("Fields[1].Nullable = false, want true")
	}
}

// The runtime serializes schema as {} when the generated query returned no
// rows, so decoding must not depend on a fields key being present.
func TestNsqlEmptyResultSet(t *testing.T) {
	spice := newNsqlTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"row_count":0,"schema":{},"data":[],"sql":"SELECT 1 WHERE false"}`)
	})

	resp, err := spice.Nsql(context.Background(), &NsqlRequest{Query: "nothing"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowCount != 0 {
		t.Errorf("RowCount = %d, want 0", resp.RowCount)
	}
	if len(resp.Schema.Fields) != 0 {
		t.Errorf("len(Schema.Fields) = %d, want 0", len(resp.Schema.Fields))
	}
	if resp.SQL != "SELECT 1 WHERE false" {
		t.Errorf("SQL = %q, want the generated query", resp.SQL)
	}
}

func TestNsqlGenerateSQL(t *testing.T) {
	var gotAccept string

	spice := newNsqlTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAccept = r.Header.Get("Accept")
		// The runtime answers this media type with the bare query text.
		_, _ = io.WriteString(w, "\n  SELECT count(*) FROM orders\n")
	})

	sql, err := spice.NsqlGenerateSQL(context.Background(), &NsqlRequest{Query: "how many orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotAccept != nsqlSQLMediaType {
		t.Errorf("Accept = %q, want %q", gotAccept, nsqlSQLMediaType)
	}
	if sql != "SELECT count(*) FROM orders" {
		t.Errorf("sql = %q, want the trimmed query", sql)
	}
}

func TestNsqlValidation(t *testing.T) {
	tests := []struct {
		name    string
		req     *NsqlRequest
		wantErr string
	}{
		{name: "nil request", req: nil, wantErr: "req is required"},
		{name: "empty query", req: &NsqlRequest{}, wantErr: "req.Query is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spice := newNsqlTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				t.Error("request should not reach the runtime")
			})

			if _, err := spice.Nsql(context.Background(), tt.req); err == nil {
				t.Fatal("expected an error, got nil")
			} else if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
			}

			if _, err := spice.NsqlGenerateSQL(context.Background(), tt.req); err == nil {
				t.Fatal("expected an error from NsqlGenerateSQL, got nil")
			}
		})
	}
}

// A missing or ambiguous model is the most common NSQL failure and the runtime
// explains it in the body. Losing that leaves the caller with a bare 400.
func TestNsqlErrorSurfacesBody(t *testing.T) {
	spice := newNsqlTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, "No model specified and no compatible LLM model is configured.")
	})

	_, err := spice.Nsql(context.Background(), &NsqlRequest{Query: "how many orders"})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "No model specified") {
		t.Errorf("error = %q, want it to carry the runtime's explanation", err)
	}
	if !strings.Contains(err.Error(), "status=400") {
		t.Errorf("error = %q, want it to carry the status code", err)
	}
}
