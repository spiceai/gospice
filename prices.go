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

func (c *SpiceClient) GetLatestPrices(ctx context.Context, pairs []string) ([]Quote, error) {
	urlBuilder := strings.Builder{}
	urlBuilder.WriteString("https://data.spiceai.io/v1/latest-prices?preview=true")
	if len(pairs) > 0 {
		pairsStr := strings.Join(pairs, ",")
		urlBuilder.WriteString(fmt.Sprintf("&pair=%s", pairsStr))
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

	// Try to decode into a slice of Quote objects
	var quotes []Quote
	err = json.NewDecoder(resp.Body).Decode(&quotes)
	if err != nil {
		// If decoding into a slice failed, try decoding into a single Quote
		var singleQuote Quote
		err = json.NewDecoder(resp.Body).Decode(&singleQuote)
		if err != nil {
			return nil, fmt.Errorf("error decoding response: %w", err)
		}
		quotes = append(quotes, singleQuote)
	}

	return quotes, nil
}

func (c *SpiceClient) GetV1Prices(ctx context.Context, pairs []string, params *QuoteParams) ([]Quote, error) {
	urlBuilder := strings.Builder{}
	urlBuilder.WriteString("https://data.spiceai.io/v1/prices?preview=true")
	if params != nil {
		if len(pairs) > 0 {
			pairsStr := strings.Join(pairs, ",")
			urlBuilder.WriteString(fmt.Sprintf("&pairs=%s", pairsStr))
		}
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

	var quotes []Quote
	err = json.NewDecoder(resp.Body).Decode(&quotes)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return quotes, nil
}

func (c *SpiceClient) GetPrices(ctx context.Context, pair string, params *QuoteParams) (*Quote, error) {
	urlBuilder := strings.Builder{}
	urlBuilder.WriteString(fmt.Sprintf("https://data.spiceai.io/v0.1/prices/%s?preview=true", pair))
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
