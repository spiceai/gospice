package gospice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeStatus(t *testing.T) {
	tests := []struct {
		name string
		// statusCode and body are what the stub runtime returns.
		statusCode int
		body       string
		wantErr    bool
		// wantErrContains, when set, must appear in the error message so a caller can
		// tell 401 from 500 without decoding the numeric code themselves.
		wantErrContains string
		want            []ConnectionDetails
	}{
		{
			name:       "all components reported",
			statusCode: http.StatusOK,
			body: `[
				{"name":"http","endpoint":"127.0.0.1:8090","status":"Ready"},
				{"name":"flight","endpoint":"127.0.0.1:50051","status":"Ready"},
				{"name":"metrics","endpoint":"N/A","status":"Disabled"},
				{"name":"opentelemetry","endpoint":"127.0.0.1:50051","status":"Ready"}
			]`,
			want: []ConnectionDetails{
				{Name: "http", Endpoint: "127.0.0.1:8090", Status: ComponentStatusReady},
				{Name: "flight", Endpoint: "127.0.0.1:50051", Status: ComponentStatusReady},
				{Name: "metrics", Endpoint: "N/A", Status: ComponentStatusDisabled},
				{Name: "opentelemetry", Endpoint: "127.0.0.1:50051", Status: ComponentStatusReady},
			},
		},
		{
			name:       "component still initializing",
			statusCode: http.StatusOK,
			body:       `[{"name":"flight","endpoint":"127.0.0.1:50051","status":"Initializing"}]`,
			want: []ConnectionDetails{
				{Name: "flight", Endpoint: "127.0.0.1:50051", Status: ComponentStatusInitializing},
			},
		},
		{
			name:            "non-200 is an error naming the status",
			statusCode:      http.StatusInternalServerError,
			body:            "boom",
			wantErr:         true,
			wantErrContains: "status=500 Internal Server Error",
		},
		{
			name:            "unauthorized is distinguishable from a server error",
			statusCode:      http.StatusUnauthorized,
			body:            "no api key",
			wantErr:         true,
			wantErrContains: "status=401 Unauthorized",
		},
		{
			name:       "malformed body is an error",
			statusCode: http.StatusOK,
			body:       `{"not":"an array"}`,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/status" {
					t.Errorf("got path %q, want /v1/status", r.URL.Path)
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

			got, err := spice.RuntimeStatus(context.Background())
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
				t.Fatalf("got %d components, want %d", len(got), len(tt.want))
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("component %d: got %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestConnectionDetailsIsReady(t *testing.T) {
	tests := []struct {
		status ComponentStatus
		want   bool
	}{
		{ComponentStatusReady, true},
		{ComponentStatusInitializing, false},
		{ComponentStatusDisabled, false},
		{ComponentStatusError, false},
		{ComponentStatusNotLoaded, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			d := ConnectionDetails{Name: "flight", Status: tt.status}
			if got := d.IsReady(); got != tt.want {
				t.Errorf("IsReady() = %v, want %v", got, tt.want)
			}
		})
	}
}
