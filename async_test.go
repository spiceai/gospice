package gospice

import (
	"context"
	"strings"
	"testing"
	"time"
)

// skipIfNoAsync skips the test when the runtime does not support async queries
// (i.e. it is not running in distributed/scheduler mode), mirroring how the
// cloud tests skip when SPICE_API_KEY is unset.
func skipIfNoAsync(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	msg := err.Error()
	for _, marker := range []string{"cluster mode", "scheduler", "Unavailable", "Unimplemented"} {
		if strings.Contains(msg, marker) {
			t.Skipf("async queries unavailable (runtime not in scheduler mode): %v", err)
		}
	}
}

func TestAsyncQueryLocal(t *testing.T) {
	spice := NewSpiceClient()
	defer func() { _ = spice.Close() }()

	if err := spice.Init(); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}

	t.Run("Submit, Wait, Results", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		query, err := spice.Query(ctx, "SELECT 1 AS n")
		skipIfNoAsync(t, err)
		if err != nil {
			t.Fatalf("error submitting async query: %v", err)
		}
		if query.ID() == "" {
			t.Fatal("expected a non-empty query ID")
		}

		status, err := query.Wait(ctx)
		if err != nil {
			t.Fatalf("error waiting for async query: %v", err)
		}
		if status != QueryStatusSucceeded {
			t.Fatalf("expected SUCCEEDED, got %s", status)
		}

		reader, err := query.Results(ctx)
		if err != nil {
			t.Fatalf("error fetching async results: %v", err)
		}
		defer reader.Release()

		var rows int64
		for reader.Next() {
			rows += reader.RecordBatch().NumRows()
		}
		if rows == 0 {
			t.Fatal("expected at least one row")
		}
		t.Logf("async query %s returned %d rows", query.ID(), rows)
	})

	t.Run("Cancel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		query, err := spice.Query(ctx, "SELECT * FROM taxi_trips")
		skipIfNoAsync(t, err)
		if err != nil {
			t.Fatalf("error submitting async query: %v", err)
		}

		if err := query.Cancel(ctx); err != nil {
			t.Fatalf("error cancelling async query: %v", err)
		}
		t.Logf("cancellation requested for query %s", query.ID())
	})
}
