package gospice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLocalRuntimeDatasetRefresh(t *testing.T) {
	spice := NewSpiceClient()
	defer func() { _ = spice.Close() }()

	if err := spice.Init(WithHttpAddress("http://127.0.0.1:8090")); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	ctx := context.Background()

	// Check if Spice is healthy
	if !spice.IsSpiceHealthy(ctx) {
		t.Fatal("Spice instance is not healthy")
	}

	// Wait for Spice to be ready (with timeout)
	timeout := time.After(120 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	ready := false
	for !ready {
		select {
		case <-timeout:
			t.Fatal("Timed out waiting for Spice to be ready")
		case <-ticker.C:
			if spice.IsSpiceReady(ctx) {
				ready = true
			}
		}
	}

	t.Run("Refresh Dataset", func(t *testing.T) {
		if err := spice.RefreshDataset(ctx, "taxi_trips", nil); err != nil {
			t.Fatalf("error refreshing dataset: %v", err)
		}
	})
}

func TestRefreshDatasetApiKeyHeader(t *testing.T) {
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

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, present = r.Header["X-Api-Key"]
				got = r.Header.Get("X-API-Key")
				w.WriteHeader(http.StatusCreated)
			}))
			t.Cleanup(server.Close)

			spice := NewSpiceClient()
			if err := WithHttpAddress(server.URL)(spice); err != nil {
				t.Fatalf("error setting http address: %v", err)
			}
			spice.apiKey = tt.apiKey

			if err := spice.RefreshDataset(context.Background(), "app_messages", nil); err != nil {
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
