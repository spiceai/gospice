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
	StartTime   time.Time
	EndTime     time.Time
	Granularity string
}

func (c *SpiceClient) GetPrices(ctx context.Context, pairs []string) (map[string]Quote, error) {
	urlBuilder := strings.Builder{}
	urlBuilder.WriteString(c.baseHttpUrl)
	urlBuilder.WriteString("/v1/prices")
	if len(pairs) > 0 {
		pairsStr := strings.Join(pairs, ",")
		urlBuilder.WriteString(fmt.Sprintf("?pairs=%s", pairsStr))
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s failed with status %d", url, resp.StatusCode)
	}

	// Try to decode into a slice of Quote objects
	var quotes map[string]Quote
	if err = json.NewDecoder(resp.Body).Decode(&quotes); err != nil {
		return nil, fmt.Errorf("error decoding response from: %w", err)
	}

	return quotes, nil
}

func (c *SpiceClient) GetHistoricalPrices(ctx context.Context, pairs []string, params *QuoteParams) (map[string][]Price, error) {
	urlBuilder := strings.Builder{}
	urlBuilder.WriteString(c.baseHttpUrl)
	urlBuilder.WriteString("/v1/prices/historical")
	if len(pairs) > 0 {
		pairsStr := strings.Join(pairs, ",")
		urlBuilder.WriteString(fmt.Sprintf("?pairs=%s", pairsStr))
	} else {
		return map[string][]Price{}, nil
	}
	if params != nil {
		if !params.StartTime.IsZero() {
			urlBuilder.WriteString(fmt.Sprintf("&start=%d", params.StartTime.Unix()))
		}
		if !params.EndTime.IsZero() {
			urlBuilder.WriteString(fmt.Sprintf("&end=%d", params.EndTime.Unix()))
		}
		if params.Granularity != "" {
			urlBuilder.WriteString(fmt.Sprintf("&granularity=%s", params.Granularity))
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
