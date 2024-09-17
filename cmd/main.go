package main

import (
	"context"
	"fmt"

	gospice "github.com/spiceai/gospice/v6"
)

func querySpiceCloud() {
	spice := gospice.NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(
		gospice.WithApiKey("3437|89d6b41cd0034cd68eea704f5e88779d"),
		gospice.WithSpiceCloudAddress(),
	); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	reader, err := spice.Query(context.Background(), "SELECT * FROM eth.recent_blocks ORDER BY number LIMIT 10")
	if err != nil {
		panic(fmt.Errorf("error querying: %w", err))
	}
	defer reader.Release()

	for reader.Next() {
		record := reader.Record()
		defer record.Release()
		fmt.Println(record)
	}
}

func querySpiceLocal() {
	spice := gospice.NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	reader, err := spice.Query(context.Background(), "SELECT * FROM taxi_trips LIMIT 10")
	if err != nil {
		panic(fmt.Errorf("error querying: %w", err))
	}
	defer reader.Release()

	for reader.Next() {
		record := reader.Record()
		defer record.Release()
		fmt.Println(record)
	}
}

// Test refreshing a local spiced dataset.
func localDatasetRefresh() {
	spice := gospice.NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(
		gospice.WithHttpAddress("http://127.0.0.1:8090"),
	); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	refresh_mode := gospice.RefreshModeFull
	sql := "SELECT * FROM test where gas_used > 20000000"
	dataset := "test"
	max_jitter := "10s"

	if err := spice.RefreshDataset(context.Background(), dataset, &gospice.DatasetRefreshApiRequest{
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
	localDatasetRefresh()
}
