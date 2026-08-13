package gospice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestListActiveQueries(t *testing.T) {
	tests := []struct {
		name string
		// statusCode and body are what the stub runtime returns.
		statusCode int
		body       string
		wantErr    bool
		// wantErrContains, when set, must appear in the error message so a caller can
		// tell a permissions problem from a server error.
		wantErrContains string
		want            []ActiveQuery
	}{
		{
			name:       "queries reported",
			statusCode: http.StatusOK,
			body: `{"queries":[
				{"query_id":"0198f0a1-9c3d-7c4e-8a11-2b3c4d5e6f70","protocol":"flight","sql_preview":"SELECT * FROM taxi_trips","started_at_ms":1750000000000},
				{"query_id":"0198f0a1-9c3d-7c4e-8a11-2b3c4d5e6f71","protocol":"http","sql_preview":"SELECT 1","started_at_ms":1750000000500}
			],"total_count":2}`,
			want: []ActiveQuery{
				{QueryID: "0198f0a1-9c3d-7c4e-8a11-2b3c4d5e6f70", Protocol: "flight", SQLPreview: "SELECT * FROM taxi_trips", StartedAtMs: 1750000000000},
				{QueryID: "0198f0a1-9c3d-7c4e-8a11-2b3c4d5e6f71", Protocol: "http", SQLPreview: "SELECT 1", StartedAtMs: 1750000000500},
			},
		},
		{
			name:       "no queries running",
			statusCode: http.StatusOK,
			body:       `{"queries":[],"total_count":0}`,
			want:       []ActiveQuery{},
		},
		{
			name:            "forbidden names the credential problem",
			statusCode:      http.StatusForbidden,
			body:            "write access required",
			wantErr:         true,
			wantErrContains: "write access",
		},
		{
			name:            "non-200 is an error naming the status",
			statusCode:      http.StatusInternalServerError,
			body:            "boom",
			wantErr:         true,
			wantErrContains: "status=500 Internal Server Error",
		},
		{
			name:       "malformed body is an error",
			statusCode: http.StatusOK,
			body:       `not json`,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/sql/active" {
					t.Errorf("got path %q, want /v1/sql/active", r.URL.Path)
				}
				if r.Method != http.MethodGet {
					t.Errorf("got method %q, want GET", r.Method)
				}
				if ua := r.Header.Get("user-agent"); ua == "" {
					t.Error("user-agent header was not set")
				}
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			spice := NewSpiceClient()
			defer func() { _ = spice.Close() }()
			if err := WithHttpAddress(srv.URL)(spice); err != nil {
				t.Fatalf("error setting http address: %v", err)
			}

			got, err := spice.ListActiveQueries(context.Background())
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				if tt.wantErrContains != "" && !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d queries, want %d", len(got), len(tt.want))
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("query %d: got %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestCancelActiveQuery(t *testing.T) {
	const queryID = "0198f0a1-9c3d-7c4e-8a11-2b3c4d5e6f70"

	tests := []struct {
		name            string
		queryID         string
		statusCode      int
		body            string
		wantErr         bool
		wantErrContains string
		// skipServer is set for cases rejected before any request is sent.
		skipServer bool
	}{
		{
			name:       "cancelled",
			queryID:    queryID,
			statusCode: http.StatusOK,
			body:       `{"query_id":"` + queryID + `","status":"cancelled"}`,
		},
		{
			name:            "empty query ID is rejected without a request",
			queryID:         "",
			wantErr:         true,
			wantErrContains: "ListActiveQueries",
			skipServer:      true,
		},
		{
			name:            "not found explains both causes",
			queryID:         queryID,
			statusCode:      http.StatusNotFound,
			body:            `{"error":"not found"}`,
			wantErr:         true,
			wantErrContains: "already finished",
		},
		{
			name:            "bad request points at ListActiveQueries",
			queryID:         "not-a-uuid",
			statusCode:      http.StatusBadRequest,
			body:            `{"error":"Invalid query_id"}`,
			wantErr:         true,
			wantErrContains: "not a valid UUID",
		},
		{
			name:            "forbidden names the credential problem",
			queryID:         queryID,
			statusCode:      http.StatusForbidden,
			body:            "write access required",
			wantErr:         true,
			wantErrContains: "write access",
		},
		{
			name:            "unexpected status names the status",
			queryID:         queryID,
			statusCode:      http.StatusInternalServerError,
			body:            "boom",
			wantErr:         true,
			wantErrContains: "status=500 Internal Server Error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requested := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requested = true
				wantPath := "/v1/sql/" + tt.queryID + "/cancel"
				if r.URL.Path != wantPath {
					t.Errorf("got path %q, want %q", r.URL.Path, wantPath)
				}
				if r.Method != http.MethodPost {
					t.Errorf("got method %q, want POST", r.Method)
				}
				if ua := r.Header.Get("user-agent"); ua == "" {
					t.Error("user-agent header was not set")
				}
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			spice := NewSpiceClient()
			defer func() { _ = spice.Close() }()
			if err := WithHttpAddress(srv.URL)(spice); err != nil {
				t.Fatalf("error setting http address: %v", err)
			}

			err := spice.CancelActiveQuery(context.Background(), tt.queryID)
			if tt.skipServer && requested {
				t.Error("expected no request to be sent")
			}
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				if tt.wantErrContains != "" && !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestActiveQueryStartedAt(t *testing.T) {
	q := ActiveQuery{StartedAtMs: 1750000000000}
	want := time.UnixMilli(1750000000000)
	if got := q.StartedAt(); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
