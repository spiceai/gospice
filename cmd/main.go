package main

import (
	"context"
	"fmt"
	"os"

	gospice "github.com/spiceai/gospice/v9"
)

func querySpiceCloud() {
	apiKey := os.Getenv("SPICE_API_KEY")
	if apiKey == "" {
		fmt.Println("SPICE_API_KEY not set; skipping Spice Cloud example")
		return
	}

	spice := gospice.NewSpiceClient()
	defer func() { _ = spice.Close() }()

	if err := spice.Init(
		gospice.WithApiKey(apiKey),
		gospice.WithSpiceCloudAddress(),
	); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	reader, err := spice.Sql(context.Background(), "SELECT * FROM eth.recent_blocks ORDER BY number LIMIT 10")
	if err != nil {
		panic(fmt.Errorf("error querying: %w", err))
	}
	defer reader.Release()

	for reader.Next() {
		fmt.Println(reader.RecordBatch())
	}
}

func querySpiceLocal() {
	spice := gospice.NewSpiceClient()
	defer func() { _ = spice.Close() }()

	if err := spice.Init(); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	reader, err := spice.Sql(context.Background(), "SELECT * FROM taxi_trips LIMIT 10")
	if err != nil {
		panic(fmt.Errorf("error querying: %w", err))
	}
	defer reader.Release()

	for reader.Next() {
		fmt.Println(reader.RecordBatch())
	}
}

// asyncQueryLocal demonstrates the async query API: Query submits the SQL for
// background execution and returns a handle used to Wait for completion and
// fetch Results. Async queries require the Spice runtime to be running in
// distributed/scheduler mode (spiced --role scheduler with
// runtime.scheduler.state_location configured).
func asyncQueryLocal() {
	spice := gospice.NewSpiceClient()
	defer func() { _ = spice.Close() }()

	if err := spice.Init(); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	ctx := context.Background()

	query, err := spice.Query(ctx, "SELECT * FROM taxi_trips LIMIT 10")
	if err != nil {
		fmt.Printf("async query submit failed (requires scheduler mode): %v\n", err)
		return
	}
	fmt.Printf("submitted async query %s\n", query.ID())

	reader, err := query.Results(ctx)
	if err != nil {
		fmt.Printf("async query failed: %v\n", err)
		return
	}
	defer reader.Release()

	for reader.Next() {
		fmt.Println(reader.RecordBatch())
	}
}

// Test refreshing a local spiced dataset.
func localDatasetRefresh() {
	spice := gospice.NewSpiceClient()
	defer func() { _ = spice.Close() }()

	if err := spice.Init(
		gospice.WithHttpAddress("http://127.0.0.1:8090"),
	); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	refresh_mode := gospice.RefreshModeFull
	sql := "SELECT * FROM test where gas_used > 20000000"
	dataset := "test"
	max_jitter := "10s"

	if err := spice.RefreshDataset(context.Background(), dataset, &gospice.DatasetRefreshRequest{
		RefreshSQL: &sql,
		Mode:       &refresh_mode,
		MaxJitter:  &max_jitter,
	}); err != nil {
		panic(fmt.Errorf("error refreshing dataset: %w", err))
	}
}

func main() {
	querySpiceCloud()
	querySpiceLocal()
	asyncQueryLocal()
	localDatasetRefresh()
}
