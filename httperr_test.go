package gospice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"
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
		{
			name:   "Nsql",
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, c *SpiceClient) error {
				_, err := c.Nsql(ctx, &NsqlRequest{Query: "how many orders"})
				return err
			},
		},
		{
			name:   "NsqlGenerateSQL",
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, c *SpiceClient) error {
				_, err := c.NsqlGenerateSQL(ctx, &NsqlRequest{Query: "how many orders"})
				return err
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

// A JSON error envelope reaches the caller unwrapped from every endpoint, not
// only the ones that never handed back the raw body.
func TestNsqlErrorIsUnwrappedLikeEveryOtherEndpoint(t *testing.T) {
	const explanation = "model 'gpt' not found"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"` + explanation + `"}`))
	}))
	t.Cleanup(server.Close)

	spice := NewSpiceClient()
	t.Cleanup(func() { _ = spice.Close() })
	if err := WithHttpAddress(server.URL)(spice); err != nil {
		t.Fatalf("error setting http address: %v", err)
	}

	_, err := spice.Nsql(context.Background(), &NsqlRequest{Query: "how many orders"})
	if err == nil {
		t.Fatal("expected an error from a 404")
	}
	if strings.Contains(err.Error(), `{"error"`) {
		t.Errorf("the JSON envelope should not reach the caller; got %q", err.Error())
	}
	if !strings.Contains(err.Error(), explanation) {
		t.Errorf("error should carry the runtime's explanation %q, got %q", explanation, err.Error())
	}
}

func TestErrorMessageIsBounded(t *testing.T) {
	oversized := strings.Repeat("x", maxErrorMessageBytes+4096)

	got := errorMessageFromBody([]byte(oversized))
	if len(got) > maxErrorMessageBytes+64 {
		t.Errorf("message should be bounded near %d bytes, got %d", maxErrorMessageBytes, len(got))
	}
	if !strings.Contains(got, "truncated") {
		t.Errorf("a cut message must say it was cut; got the tail %q", got[max(0, len(got)-64):])
	}

	// A body that fits is returned whole, marker and all.
	exact := strings.Repeat("y", maxErrorMessageBytes)
	if got := errorMessageFromBody([]byte(exact)); got != exact {
		t.Errorf("a message at the bound should be returned unchanged; len=%d, truncated=%v",
			len(got), strings.Contains(got, "truncated"))
	}
}

// Cutting at a fixed byte offset can land inside a multi-byte rune, which would
// put invalid UTF-8 into the error a caller prints.
func TestTruncationCutsOnARuneBoundary(t *testing.T) {
	// "→" is three bytes, so repeating it guarantees the bound falls mid-rune.
	body := strings.Repeat("→", maxErrorMessageBytes)

	got := errorMessageFromBody([]byte(body))
	if !strings.Contains(got, "truncated") {
		t.Fatalf("expected the message to be truncated, got %d bytes", len(got))
	}
	if !utf8.ValidString(got) {
		t.Error("truncation left invalid UTF-8 in the message")
	}
}

// The read itself is bounded, so a server streaming an unbounded error body
// cannot be buffered in full before the bound is applied. Measured on what the
// server got to send rather than on the message: the message bound alone would
// keep the error small while the whole body was still read into memory.
func TestRuntimeErrorMessageBoundsTheRead(t *testing.T) {
	const offered = 32 << 20

	var served atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		chunk := []byte(strings.Repeat("z", 4096))
		for served.Load() < offered {
			n, err := w.Write(chunk)
			served.Add(int64(n))
			if err != nil {
				return
			}
		}
	}))

	spice := NewSpiceClient()
	t.Cleanup(func() { _ = spice.Close() })
	if err := WithHttpAddress(server.URL)(spice); err != nil {
		t.Fatalf("error setting http address: %v", err)
	}

	err := spice.RefreshDataset(context.Background(), "orders", nil)
	// Close blocks until the handler has returned, so served is settled and
	// race-free by the time it is read.
	server.Close()

	if err == nil {
		t.Fatal("expected an error from a 500")
	}
	if got := served.Load(); got >= offered {
		t.Errorf("the whole %d-byte body was read; the read is not bounded (server sent %d)", offered, got)
	}
	if len(err.Error()) > maxErrorMessageBytes+256 {
		t.Errorf("the error should carry at most the bound; got %d bytes", len(err.Error()))
	}
}
