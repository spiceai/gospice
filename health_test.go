package gospice

import (
	"context"
	"os"
	"testing"
)

func TestIsSpiceHealthy(t *testing.T) {
	t.Run("Local - Health Check", func(t *testing.T) {
		spice := NewSpiceClient()
		defer func() {
			if err := spice.Close(); err != nil {
				t.Logf("warning: failed to close SpiceClient: %v", err)
			}
		}()

		if err := spice.Init(); err != nil {
			t.Fatalf("error initializing SpiceClient: %v", err)
		}

		ctx := context.Background()
		isHealthy := spice.IsSpiceHealthy(ctx)

		// We don't assert true/false since local runtime may not be running
		// This test just verifies the method works without errors
		t.Logf("Local Spice health status: %v", isHealthy)
	})

	t.Run("Cloud - Health Check", func(t *testing.T) {
		spice := NewSpiceClient()
		defer func() {
			if err := spice.Close(); err != nil {
				t.Logf("warning: failed to close SpiceClient: %v", err)
			}
		}()

		var ApiKey string
		if v, exists := os.LookupEnv("SPICE_API_KEY"); exists {
			ApiKey = v
		} else {
			ApiKey = TEST_API_KEY
		}

		if err := spice.Init(WithApiKey(ApiKey), WithSpiceCloudAddress()); err != nil {
			t.Fatalf("error initializing SpiceClient: %v", err)
		}

		ctx := context.Background()
		isHealthy := spice.IsSpiceHealthy(ctx)

		t.Logf("Spice Cloud health status: %v", isHealthy)
	})
}

func TestIsSpiceReady(t *testing.T) {
	t.Run("Cloud - Ready Check", func(t *testing.T) {
		spice := NewSpiceClient()
		defer func() {
			if err := spice.Close(); err != nil {
				t.Logf("warning: failed to close SpiceClient: %v", err)
			}
		}()

		var ApiKey string
		if v, exists := os.LookupEnv("SPICE_API_KEY"); exists {
			ApiKey = v
		} else {
			ApiKey = TEST_API_KEY
		}

		if err := spice.Init(WithApiKey(ApiKey), WithSpiceCloudAddress()); err != nil {
			t.Fatalf("error initializing SpiceClient: %v", err)
		}

		ctx := context.Background()
		isReady := spice.IsSpiceReady(ctx)

		t.Logf("Spice Cloud ready status: %v", isReady)
	})

	t.Run("Local - Ready Check", func(t *testing.T) {
		spice := NewSpiceClient()
		defer func() {
			if err := spice.Close(); err != nil {
				t.Logf("warning: failed to close SpiceClient: %v", err)
			}
		}()

		if err := spice.Init(); err != nil {
			t.Fatalf("error initializing SpiceClient: %v", err)
		}

		ctx := context.Background()

		isHealthy := spice.IsSpiceHealthy(ctx)
		isReady := spice.IsSpiceReady(ctx)

		t.Logf("Local Spice health status: %v", isHealthy)
		t.Logf("Local Spice ready status: %v", isReady)
	})
}
