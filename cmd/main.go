package main

import (
	"context"
	"fmt"

	gospice "github.com/spiceai/gospice/v8"
)

func querySpiceCloud() {
	spice := gospice.NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			fmt.Printf("warning: failed to close SpiceClient: %v\n", err)
		}
	}()

	if err := spice.Init(
		gospice.WithApiKey("3437|89d6b41cd0034cd68eea704f5e88779d"),
		gospice.WithSpiceCloudAddress(),
	); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	reader, err := spice.Query(context.Background(), "SELECT * FROM tpch.customer ORDER BY c_custkey LIMIT 10")
	if err != nil {
		panic(fmt.Errorf("error querying: %w", err))
	}
	defer reader.Release()

	for reader.Next() {
		record := reader.RecordBatch()
		defer record.Release()
		fmt.Println(record)
	}
}

func querySpiceCloudWithParams() {
	spice := gospice.NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			fmt.Printf("warning: failed to close SpiceClient: %v\n", err)
		}
	}()

	if err := spice.Init(
		gospice.WithApiKey("3437|89d6b41cd0034cd68eea704f5e88779d"),
		gospice.WithSpiceCloudAddress(),
	); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	// Using parameterized query (recommended for queries with parameters)
	minCustKey := int64(100)
	reader, err := spice.SqlWithParams(
		context.Background(),
		"SELECT * FROM tpch.customer WHERE c_custkey > $1 ORDER BY c_custkey LIMIT 10",
		minCustKey,
	)
	if err != nil {
		panic(fmt.Errorf("error querying: %w", err))
	}
	defer reader.Release()

	fmt.Println("TPC-H customers with customer key >", minCustKey)
	for reader.Next() {
		record := reader.RecordBatch()
		defer record.Release()
		fmt.Println(record)
	}
}

func querySpiceLocal() {
	spice := gospice.NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			fmt.Printf("warning: failed to close SpiceClient: %v\n", err)
		}
	}()

	if err := spice.Init(); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	reader, err := spice.Query(context.Background(), "SELECT * FROM taxi_trips LIMIT 10")
	if err != nil {
		panic(fmt.Errorf("error querying: %w", err))
	}
	defer reader.Release()

	for reader.Next() {
		record := reader.RecordBatch()
		defer record.Release()
		fmt.Println(record)
	}
}

func querySpiceLocalWithParams() {
	spice := gospice.NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			fmt.Printf("warning: failed to close SpiceClient: %v\n", err)
		}
	}()

	if err := spice.Init(); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	// Using parameterized query for filtering
	minDistance := 5.0
	minFare := 20.0
	reader, err := spice.SqlWithParams(
		context.Background(),
		"SELECT * FROM taxi_trips WHERE trip_distance > $1 AND fare_amount > $2 LIMIT 10",
		minDistance,
		minFare,
	)
	if err != nil {
		panic(fmt.Errorf("error querying: %w", err))
	}
	defer reader.Release()

	fmt.Printf("Taxi trips with distance > %.1f and fare > $%.2f\n", minDistance, minFare)
	for reader.Next() {
		record := reader.RecordBatch()
		defer record.Release()
		fmt.Println(record)
	}
}

// Test refreshing a local spiced dataset.
func localDatasetRefresh() {
	spice := gospice.NewSpiceClient()
	defer func() {
		if err := spice.Close(); err != nil {
			fmt.Printf("warning: failed to close SpiceClient: %v\n", err)
		}
	}()

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
	// Examples using traditional Flight SQL queries
	querySpiceCloud()
	querySpiceLocal()

	// Examples using ADBC with parameterized queries (recommended)
	querySpiceCloudWithParams()
	querySpiceLocalWithParams()

	// Dataset refresh example
	localDatasetRefresh()
}
