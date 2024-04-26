package main

import (
	"context"
	"fmt"

	gospice "github.com/spiceai/gospice/v5"
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

func main() {
	querySpiceCloud()
	querySpiceLocal()
}
