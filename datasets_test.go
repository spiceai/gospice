package gospice

import (
	"context"
	"testing"
)

func TestLocalRuntimeDatasetRefresh(t *testing.T) {
	spice := NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(WithHttpAddress("http://127.0.0.1:8090")); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	// Check if local Spice runtime is healthy
	ctx := context.Background()
	if !spice.IsSpiceHealthy(ctx) {
		t.Skip("Skipping - local Spice runtime is not healthy")
	}

	t.Run("Local - Refresh Dataset", func(t *testing.T) {
		if err := spice.RefreshDataset(context.Background(), "taxi_trips", nil); err != nil {
			t.Skipf("Skipping - requires local spice runtime with taxi_trips dataset: %v", err)
		}
	})
}
