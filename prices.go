package gospice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Quote struct {
	Pair   string  `json:"pair"`
	Prices []Price `json:"prices"`
}

type Price struct {
	Timestamp string  `json:"timestamp"`
	Price     float64 `json:"price"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Open      float64 `json:"open"`
	Close     float64 `json:"close"`
}

type QuoteParams struct {
	startTime   time.Time
	endTime     time.Time
	granularity string
}

func (c *SpiceClient) GetPrices(ctx context.Context, pair string, params *QuoteParams) (*Quote, error) {
	urlBuilder := strings.Builder{}
	urlBuilder.WriteString(fmt.Sprintf("https://data.spiceai.io/v0.1/prices/%s?preview=true", pair))
	if params != nil {
		if !params.startTime.IsZero() {
			urlBuilder.WriteString(fmt.Sprintf("&start=%d", params.startTime.Unix()))
		}
		if !params.endTime.IsZero() {
			urlBuilder.WriteString(fmt.Sprintf("&end=%d", params.endTime.Unix()))
		}
		if params.granularity != "" {
			urlBuilder.WriteString(fmt.Sprintf("&granularity=%s", params.granularity))
		}
	}

	url := urlBuilder.String()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("X-API-Key", c.apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gospice 0.1")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s failed with status %d", url, resp.StatusCode)
	}

	var quote Quote
	err = json.NewDecoder(resp.Body).Decode(&quote)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &quote, nil
}
