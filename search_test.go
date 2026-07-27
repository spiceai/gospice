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

// newTestSearchClient returns a client pointed at srv, without dialing Flight.
func newTestSearchClient(srv *httptest.Server) *SpiceClient {
	c := NewSpiceClient()
	c.baseHttpUrl = srv.URL
	return c
}

func TestSearchRequestEncoding(t *testing.T) {
	tests := []struct {
		name string
		req  *SearchRequest
		want map[string]any
	}{
		{
			name: "text only omits optional fields",
			req:  &SearchRequest{Text: "tickets to Tokyo"},
			want: map[string]any{"text": "tickets to Tokyo"},
		},
		{
			name: "datasets and limit",
			req: &SearchRequest{
				Text:     "tickets to Tokyo",
				Datasets: []string{"app_messages"},
				Limit:    3,
			},
			want: map[string]any{
				"text":     "tickets to Tokyo",
				"datasets": []any{"app_messages"},
				"limit":    float64(3),
			},
		},
		{
			name: "where predicate uses the wire name",
			req: &SearchRequest{
				Text:  "tickets",
				Where: "city = 'Tokyo'",
			},
			want: map[string]any{
				"text":  "tickets",
				"where": "city = 'Tokyo'",
			},
		},
		{
			name: "keywords make the search hybrid",
			req: &SearchRequest{
				Text:              "tickets",
				AdditionalColumns: []string{"timestamp"},
				Keywords:          []string{"plane", "tickets"},
			},
			want: map[string]any{
				"text":               "tickets",
				"additional_columns": []any{"timestamp"},
				"keywords":           []any{"plane", "tickets"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("error reading request body: %v", err)
				}
				if err := json.Unmarshal(body, &got); err != nil {
					t.Errorf("error unmarshaling request body: %v", err)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"results":[],"duration_ms":1}`))
			}))
			defer srv.Close()

			if _, err := newTestSearchClient(srv).Search(context.Background(), tt.req); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(got) != len(tt.want) {
				t.Errorf("encoded %d fields, want %d: got %v", len(got), len(tt.want), got)
			}
			for k, want := range tt.want {
				gotVal, ok := got[k]
				if !ok {
					t.Errorf("missing field %q in %v", k, got)
					continue
				}
				if gotJSON, _ := json.Marshal(gotVal); string(gotJSON) != mustJSON(t, want) {
					t.Errorf("field %q = %v, want %v", k, gotVal, want)
				}
			}
		})
	}
}

func TestSearchMethodAndPath(t *testing.T) {
	var method, path, contentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, contentType = r.Method, r.URL.Path, r.Header.Get("Content-Type")
		_, _ = w.Write([]byte(`{"results":[],"duration_ms":0}`))
	}))
	defer srv.Close()

	if _, err := newTestSearchClient(srv).Search(context.Background(), &SearchRequest{Text: "x"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if method != http.MethodPost {
		t.Errorf("method = %q, want POST", method)
	}
	if path != "/v1/search" {
		t.Errorf("path = %q, want /v1/search", path)
	}
	if contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
}

func TestSearchResponseDecoding(t *testing.T) {
	// A response in the runtime's wire format: the score is `_score`, and
	// data/primary_key/metadata are omitted entirely when empty.
	const body = `{
		"results": [
			{
				"matches": {"message": ["I booked us some tickets"]},
				"dataset": "app_messages",
				"primary_key": {"id": "6fd5a215-0881-421d-ace0-b293b83452b5"},
				"data": {"timestamp": 1724716542},
				"_score": 0.914321
			},
			{
				"matches": {"message": ["direct to Narita"]},
				"dataset": "app_messages",
				"_score": 0.83221
			}
		],
		"duration_ms": 42
	}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	resp, err := newTestSearchClient(srv).Search(context.Background(), &SearchRequest{Text: "tickets"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.DurationMs != 42 {
		t.Errorf("DurationMs = %d, want 42", resp.DurationMs)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("got %d results, want 2", len(resp.Results))
	}

	first := resp.Results[0]
	if first.Dataset != "app_messages" {
		t.Errorf("Dataset = %q, want app_messages", first.Dataset)
	}
	if first.Score != 0.914321 {
		t.Errorf("Score = %v, want 0.914321", first.Score)
	}
	if got := first.PrimaryKey["id"]; got != "6fd5a215-0881-421d-ace0-b293b83452b5" {
		t.Errorf("PrimaryKey[id] = %v", got)
	}
	if got := first.Data["timestamp"]; got != float64(1724716542) {
		t.Errorf("Data[timestamp] = %v", got)
	}
	if got := first.Matches["message"]; len(got) != 1 || got[0] != "I booked us some tickets" {
		t.Errorf("Matches[message] = %v", got)
	}

	// Omitted objects decode to nil, which is safe to read from in Go.
	second := resp.Results[1]
	if second.Data != nil {
		t.Errorf("Data = %v, want nil for an omitted field", second.Data)
	}
	if got := second.Data["timestamp"]; got != nil {
		t.Errorf("reading an omitted map should yield nil, got %v", got)
	}
}

func TestSearchValidation(t *testing.T) {
	tests := []struct {
		name    string
		req     *SearchRequest
		wantErr string
	}{
		{name: "nil request", req: nil, wantErr: "search request is required"},
		{name: "empty text", req: &SearchRequest{}, wantErr: "Text is required"},
		{name: "empty text with datasets", req: &SearchRequest{Datasets: []string{"a"}}, wantErr: "Text is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Error("request should not reach the runtime when validation fails")
			}))
			defer srv.Close()

			_, err := newTestSearchClient(srv).Search(context.Background(), tt.req)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestSearchSurfacesRuntimeError(t *testing.T) {
	// The runtime answers some failures with JSON and others with plain text. Both
	// carry the part that tells the caller what to fix, so both must survive.
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "json error body",
			body: `{"error":"No data sources provided"}`,
			want: "No data sources provided",
		},
		{
			name: "plain text body",
			body: "Search cannot be run on nation because it has no embeddings or full text search indexes.",
			want: "no embeddings or full text search indexes",
		},
		{
			name: "empty body",
			body: "",
			want: "(no response body)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			_, err := newTestSearchClient(srv).Search(context.Background(), &SearchRequest{Text: "x"})
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.want)
			}
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("error marshaling %v: %v", v, err)
	}
	return string(b)
}
