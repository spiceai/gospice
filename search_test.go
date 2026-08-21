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

// newSearchTestClient returns a client pointed at handler, without dialing
// Flight - Search only uses the HTTP control plane.
func newSearchTestClient(t *testing.T, handler http.HandlerFunc) *SpiceClient {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	spice := NewSpiceClient()
	if err := WithHttpAddress(server.URL)(spice); err != nil {
		t.Fatalf("error setting http address: %v", err)
	}
	return spice
}

func TestSearchRequestEncoding(t *testing.T) {
	limit := 3
	where := "user_id = 42"

	tests := []struct {
		name string
		req  *SearchRequest
		want map[string]any
	}{
		{
			name: "text only",
			req:  &SearchRequest{Text: "tokyo"},
			want: map[string]any{"text": "tokyo"},
		},
		{
			name: "all options",
			req: &SearchRequest{
				Text:              "tokyo",
				Datasets:          []string{"app_messages"},
				Limit:             &limit,
				Where:             &where,
				AdditionalColumns: []string{"timestamp"},
				Keywords:          []string{"plane", "tickets"},
			},
			want: map[string]any{
				"text":               "tokyo",
				"datasets":           []any{"app_messages"},
				"limit":              float64(3),
				"where":              "user_id = 42",
				"additional_columns": []any{"timestamp"},
				"keywords":           []any{"plane", "tickets"},
			},
		},
		{
			// The runtime rejects an empty dataset list with a 400, so an empty
			// slice must be omitted rather than sent.
			name: "empty datasets omitted",
			req:  &SearchRequest{Text: "tokyo", Datasets: []string{}},
			want: map[string]any{"text": "tokyo"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got map[string]any

			spice := newSearchTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("error reading request body: %v", err)
				}
				if err := json.Unmarshal(body, &got); err != nil {
					t.Errorf("error decoding request body: %v", err)
				}
				_, _ = io.WriteString(w, `{"results":[],"duration_ms":0}`)
			})

			if _, err := spice.Search(context.Background(), tt.req); err != nil {
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

func TestSearchRequestPath(t *testing.T) {
	var gotPath, gotMethod string

	spice := newSearchTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		_, _ = io.WriteString(w, `{"results":[],"duration_ms":0}`)
	})

	if _, err := spice.Search(context.Background(), &SearchRequest{Text: "tokyo"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotPath != "/v1/search" {
		t.Errorf("path = %q, want %q", gotPath, "/v1/search")
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want %q", gotMethod, http.MethodPost)
	}
}

func TestSearchResponseDecoding(t *testing.T) {
	body := `{
		"results": [
			{
				"matches": {"message": ["I booked us some tickets", "direct to Narita"]},
				"dataset": "app_messages",
				"primary_key": {"id": "6fd5a215"},
				"data": {"timestamp": 1724716542},
				"metadata": {"chunk": 2},
				"_score": 0.914321
			},
			{
				"matches": {"message": ["we're sitting together"]},
				"dataset": "app_messages",
				"_score": 0.787654
			}
		],
		"duration_ms": 42
	}`

	spice := newSearchTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	})

	resp, err := spice.Search(context.Background(), &SearchRequest{Text: "tokyo"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.DurationMs != 42 {
		t.Errorf("DurationMs = %d, want 42", resp.DurationMs)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("len(Results) = %d, want 2", len(resp.Results))
	}

	first := resp.Results[0]
	if first.Dataset != "app_messages" {
		t.Errorf("Dataset = %q, want %q", first.Dataset, "app_messages")
	}
	if first.Score != 0.914321 {
		t.Errorf("Score = %v, want 0.914321", first.Score)
	}
	// One column can contribute several chunks to a single match.
	if len(first.Matches["message"]) != 2 {
		t.Errorf("len(Matches[message]) = %d, want 2", len(first.Matches["message"]))
	}
	if first.PrimaryKey["id"] != "6fd5a215" {
		t.Errorf("PrimaryKey[id] = %v, want %q", first.PrimaryKey["id"], "6fd5a215")
	}
	// Numbers decode as json.Number rather than float64 so that a 64-bit value
	// is not rounded on the way in; see TestSearchPreservesLargeIntegers.
	if got, ok := first.Data["timestamp"].(json.Number); !ok || got.String() != "1724716542" {
		t.Errorf("Data[timestamp] = %#v, want json.Number(\"1724716542\")", first.Data["timestamp"])
	}
	if got, ok := first.Metadata["chunk"].(json.Number); !ok || got.String() != "2" {
		t.Errorf("Metadata[chunk] = %#v, want json.Number(\"2\")", first.Metadata["chunk"])
	}

	// The runtime omits data, primary_key, and metadata from a match that has
	// none, and an absent JSON key leaves the map nil. Assert nil rather than
	// len == 0, which would also pass for an allocated empty map and so would
	// not pin down what a caller actually receives.
	second := resp.Results[1]
	if second.PrimaryKey != nil {
		t.Errorf("omitted primary_key should decode nil, got %v", second.PrimaryKey)
	}
	if second.Data != nil {
		t.Errorf("omitted data should decode nil, got %v", second.Data)
	}
	if second.Metadata != nil {
		t.Errorf("omitted metadata should decode nil, got %v", second.Metadata)
	}
	// A nil map is still safe to read, which is what the doc comment promises.
	if got := second.PrimaryKey["id"]; got != nil {
		t.Errorf("reading a nil PrimaryKey should yield nil, got %v", got)
	}
}

func TestSearchValidation(t *testing.T) {
	zero := 0
	negative := -1

	tests := []struct {
		name    string
		req     *SearchRequest
		wantErr string
	}{
		{name: "nil request", req: nil, wantErr: "req is required"},
		{name: "empty text", req: &SearchRequest{}, wantErr: "req.Text is required"},
		{name: "zero limit", req: &SearchRequest{Text: "tokyo", Limit: &zero}, wantErr: "greater than 0"},
		{name: "negative limit", req: &SearchRequest{Text: "tokyo", Limit: &negative}, wantErr: "greater than 0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			spice := newSearchTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				called = true
				_, _ = io.WriteString(w, `{"results":[],"duration_ms":0}`)
			})

			_, err := spice.Search(context.Background(), tt.req)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.wantErr)
			}
			if called {
				t.Error("expected no request to be sent")
			}
		})
	}
}

func TestSearchErrorSurfacesRuntimeMessage(t *testing.T) {
	spice := newSearchTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, "No data sources provided")
	})

	_, err := spice.Search(context.Background(), &SearchRequest{Text: "tokyo"})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "No data sources provided") {
		t.Errorf("error = %q, want it to carry the runtime's message", err.Error())
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error = %q, want it to carry the status code", err.Error())
	}
}

func TestSearchMalformedResponse(t *testing.T) {
	spice := newSearchTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "not json")
	})

	_, err := spice.Search(context.Background(), &SearchRequest{Text: "tokyo"})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "error decoding response") {
		t.Errorf("error = %q, want a decode error", err.Error())
	}
}

func TestSearchApiKeyHeader(t *testing.T) {
	// An empty X-API-Key is not the same as no X-API-Key: auth middleware can
	// read the former as a supplied-but-invalid credential.
	tests := []struct {
		name   string
		apiKey string
		want   string
	}{
		{name: "no key omits the header", apiKey: "", want: ""},
		{name: "key is sent", apiKey: "test-app|test-key", want: "test-app|test-key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			var present bool

			spice := newSearchTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, present = r.Header["X-Api-Key"]
				got = r.Header.Get("X-API-Key")
				_, _ = io.WriteString(w, `{"results":[],"duration_ms":0}`)
			})
			spice.apiKey = tt.apiKey

			if _, err := spice.Search(context.Background(), &SearchRequest{Text: "tokyo"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.want == "" && present {
				t.Error("X-API-Key should be absent when no key is configured")
			}
			if got != tt.want {
				t.Errorf("X-API-Key = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSearchErrorSurfacesJSONMessage(t *testing.T) {
	// The runtime reports some failures as JSON and others as plain text, so the
	// JSON envelope has to be unwrapped rather than printed raw.
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "json error body is unwrapped",
			body: `{"error":"dataset app_messages has no embeddings"}`,
			want: "dataset app_messages has no embeddings",
		},
		{
			name: "empty body is named",
			body: "",
			want: "(no response body)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spice := newSearchTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, tt.body)
			})

			_, err := spice.Search(context.Background(), &SearchRequest{Text: "tokyo"})
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.want)
			}
			if strings.Contains(err.Error(), `{"error"`) {
				t.Errorf("error = %q, want the JSON envelope unwrapped", err.Error())
			}
		})
	}
}
