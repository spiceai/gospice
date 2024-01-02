package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/spiceai/gospice/v4"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	spiceClient := gospice.NewSpiceClient()
	err := spiceClient.Init("3234|ddffd2b077ad4ba780b87ecf60447507")
	if err != nil {
		log.Fatal(err)
	}

	query := "SELECT block_epoch FROM goerli.beacon.recent_slots ORDER BY block_slot DESC LIMIT 1"
	for {
		time.Sleep(2 * time.Second)
		reader, err := spiceClient.Query(context.Background(), query)
		reader.Release()
		if err != nil {
			log.Fatal(err)
		}
	}
}
