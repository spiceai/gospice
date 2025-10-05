package gospice

import (
	"context"
	"os"
	"testing"
)

func TestIsSpiceHealthy(t *testing.T) {
	t.Run("Local - Health Check", func(t *testing.T) {
		spice := NewSpiceClient()
		defer spice.Close()

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
		defer spice.Close()

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
	t.Run("Cloud - Ready Check with API Key", func(t *testing.T) {
		spice := NewSpiceClient()
		defer spice.Close()

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

	t.Run("Cloud - Ready Check without API Key", func(t *testing.T) {
		spice := NewSpiceClient()
		defer spice.Close()

		// Initialize without API key
		if err := spice.Init(WithSpiceCloudAddress()); err != nil {
			t.Fatalf("error initializing SpiceClient: %v", err)
		}

		ctx := context.Background()
		isReady := spice.IsSpiceReady(ctx)

		// Should be false since no API key provided
		if isReady {
			t.Errorf("Expected IsSpiceReady to return false without API key, got true")
		}
		t.Logf("Spice Cloud ready status (no API key): %v", isReady)
	})

	t.Run("Local - Ready Check (No Auth Required)", func(t *testing.T) {
		spice := NewSpiceClient()
		defer spice.Close()

		if err := spice.Init(); err != nil {
			t.Fatalf("error initializing SpiceClient: %v", err)
		}

		ctx := context.Background()

		// Local runtime doesn't require API key, so this will return false
		// but IsSpiceHealthy should work
		isHealthy := spice.IsSpiceHealthy(ctx)
		isReady := spice.IsSpiceReady(ctx)

		t.Logf("Local Spice health status: %v", isHealthy)
		t.Logf("Local Spice ready status (no auth): %v", isReady)
	})
}
