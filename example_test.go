package gospice_test

import (
	"context"
	"fmt"
	"os"

	"github.com/spiceai/gospice/v9"
)

// The README's quickstart, kept here so the compiler checks it. Examples
// without an Output comment are compiled by go test and go vet but not run, so
// none of these needs a Spice runtime.

func ExampleSpiceClient_Sql() {
	spice := gospice.NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(
		gospice.WithApiKey(os.Getenv("SPICE_API_KEY")),
		gospice.WithSpiceCloudAddress(),
	); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	reader, err := spice.Sql(context.Background(), "SELECT 1")
	if err != nil {
		panic(fmt.Errorf("error querying: %w", err))
	}
	defer reader.Release()

	// The reader owns each record batch and releases it on the next call to
	// Next. Call Retain on a batch to keep it longer, and Release it after.
	for reader.Next() {
		record := reader.RecordBatch()
		fmt.Println(record)
	}
	if err := reader.Err(); err != nil {
		panic(fmt.Errorf("error reading results: %w", err))
	}
}

func ExampleSpiceClient_Init_local() {
	spice := gospice.NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(
		gospice.WithFlightAddress("grpc://localhost:50052"),
	); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}
}

func ExampleSpiceClient_SetMaxRetries() {
	spice := gospice.NewSpiceClient()
	spice.SetMaxRetries(5) // Setting to 0 will disable retries
}
