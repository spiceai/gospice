package gospice

import (
	"context"
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
		t.Skip("Skipping - Spice instance is not healthy")
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
