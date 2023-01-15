package main

import (
	"context"
	"fmt"
)

func main() {
	spice := NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(context.Background(), "3437|89d6b41cd0034cd68eea704f5e88779d"); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	reader, err := spice.Query("SELECT * FROM eth.recent_blocks ORDER BY number LIMIT 10")
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
