package gospice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestErrorMessageFromBody(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "JSON error is unwrapped",
			body: `{"error":"dataset 'orders' not found"}`,
			want: "dataset 'orders' not found",
		},
		{
			name: "plain text is reported as-is",
			body: "  No data sources provided\n",
			want: "No data sources provided",
		},
		{
			name: "JSON without an error field falls back to the raw body",
			body: `{"detail":"nope"}`,
			want: `{"detail":"nope"}`,
		},
		{
			name: "an empty body says so rather than reading as no explanation",
			body: "   \n",
			want: "(no response body)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorMessageFromBody([]byte(tt.body)); got != tt.want {
				t.Errorf("errorMessageFromBody() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRuntimeErrorMessageReportsAnUnreadableBody(t *testing.T) {
	// Declares more body than it sends and then hangs up, so reading the body
	// fails part-way. The status is already known by then; without this the
	// transport failure was reported as an absent explanation.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "64")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("short"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
	}))
	t.Cleanup(server.Close)

	spice := NewSpiceClient()
	t.Cleanup(func() { _ = spice.Close() })
	if err := WithHttpAddress(server.URL)(spice); err != nil {
		t.Fatalf("error setting http address: %v", err)
	}

	err := spice.RefreshDataset(context.Background(), "app_messages", nil)
	if err == nil {
		t.Fatal("expected an error from a 500 with a truncated body")
	}
	if !strings.Contains(err.Error(), "could not be read") {
		t.Errorf("the read failure should be reported, not discarded; got %q", err.Error())
	}
}

// The runtime explains an HTTP failure in the response body. Every method below
// used to report only the status code, so the one part of the response saying
// what to fix never reached the caller.
func TestHTTPErrorsCarryTheRuntimeExplanation(t *testing.T) {
	const explanation = "dataset 'orders' has no acceleration configured"

	tests := []struct {
		name   string
		status int
		call   func(ctx context.Context, c *SpiceClient) error
	}{
		{
			name:   "RefreshDataset",
			status: http.StatusNotFound,
			call: func(ctx context.Context, c *SpiceClient) error {
				return c.RefreshDataset(ctx, "orders", nil)
			},
		},
		{
			name:   "RuntimeStatus",
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, c *SpiceClient) error {
				_, err := c.RuntimeStatus(ctx)
				return err
			},
		},
		{
			name:   "ListActiveQueries",
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, c *SpiceClient) error {
				_, err := c.ListActiveQueries(ctx)
				return err
			},
		},
		{
			name:   "CancelActiveQuery",
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, c *SpiceClient) error {
				return c.CancelActiveQuery(ctx, "3f2504e0-4f89-11d3-9a0c-0305e82c3301")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(`{"error":"` + explanation + `"}`))
			}))
			t.Cleanup(server.Close)

			spice := NewSpiceClient()
			t.Cleanup(func() { _ = spice.Close() })
			if err := WithHttpAddress(server.URL)(spice); err != nil {
				t.Fatalf("error setting http address: %v", err)
			}

			err := tt.call(context.Background(), spice)
			if err == nil {
				t.Fatalf("expected an error from status %d", tt.status)
			}
			if !strings.Contains(err.Error(), explanation) {
				t.Errorf("error should carry the runtime's explanation %q, got %q", explanation, err.Error())
			}
		})
	}
}
