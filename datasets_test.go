package gospice

import (
	"context"
	"fmt"
	"testing"
)

func TestLocalRuntimeDatasetRefresh(t *testing.T) {
	spice := NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(WithHttpAddress("http://127.0.0.1:8090")); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	t.Run("Refresh Dataset", func(t *testing.T) {
		if err := spice.RefreshDataset(context.Background(), "taxi_trips", nil); err != nil {
			panic(fmt.Errorf("error refreshing dataset: %w", err))
		}
	})
}
