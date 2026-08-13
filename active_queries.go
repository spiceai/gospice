package gospice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ActiveQuery describes a synchronous query currently running on the runtime.
type ActiveQuery struct {
	// QueryID is assigned by the runtime and is what CancelActiveQuery takes.
	QueryID string `json:"query_id"`
	// Protocol the query arrived on, such as "flight" or "http".
	Protocol string `json:"protocol"`
	// SQLPreview is the query's SQL, truncated by the runtime for display.
	SQLPreview string `json:"sql_preview"`
	// StartedAtMs is when the query started, in milliseconds since the Unix epoch.
	StartedAtMs int64 `json:"started_at_ms"`
}

// StartedAt returns the query's start time.
func (q ActiveQuery) StartedAt() time.Time {
	return time.UnixMilli(q.StartedAtMs)
}

type activeQueriesResponse struct {
	Queries    []ActiveQuery `json:"queries"`
	TotalCount int           `json:"total_count"`
}

type cancelActiveQueryResponse struct {
	QueryID string `json:"query_id"`
	Status  string `json:"status"`
}

// ListActiveQueries returns the synchronous queries this client currently has running.
//
// Synchronous queries are the ones started by Sql, SqlWithParams, FlightSQL, NSQL and
// Search. Async query jobs are listed separately and are only available when the
// runtime runs in cluster mode.
//
// The runtime does not return a query's ID to the client that submitted it, so this is
// how to find the ID that CancelActiveQuery needs. Results are scoped to this client —
// another caller's in-flight queries are never listed.
func (c *SpiceClient) ListActiveQueries(ctx context.Context) ([]ActiveQuery, error) {
	url := fmt.Sprintf("%s/v1/sql/active", c.baseHttpUrl)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req = req.WithContext(c.traceHttpRequest(ctx, "ListActiveQueries", req))

	req.Header.Set("Accept", "application/json")
	req.Header.Set("user-agent", c.userAgent)
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("GET %s failed: the configured API key does not allow listing queries, use a key with write access", url)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s failed with status=%d %s", url, resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	var decoded activeQueriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("error decoding active queries response: %w", err)
	}

	return decoded.Queries, nil
}

// CancelActiveQuery cancels a running synchronous query by ID.
//
// queryID comes from ListActiveQueries. Cancellation is scoped to this client: an ID
// belonging to another caller is reported as not found rather than cancelled.
//
// To cancel an async query job instead, use AsyncQuery.Cancel.
func (c *SpiceClient) CancelActiveQuery(ctx context.Context, queryID string) error {
	if queryID == "" {
		return fmt.Errorf("queryID is required, use ListActiveQueries to find one")
	}

	url := fmt.Sprintf("%s/v1/sql/%s/cancel", c.baseHttpUrl, queryID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req = req.WithContext(c.traceHttpRequest(ctx, "CancelActiveQuery", req))

	req.Header.Set("Accept", "application/json")
	req.Header.Set("user-agent", c.userAgent)
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		var decoded cancelActiveQueryResponse
		if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
			return fmt.Errorf("error decoding cancel response: %w", err)
		}
		return nil
	case http.StatusBadRequest:
		return fmt.Errorf("query ID %q is not a valid UUID, use the QueryID from ListActiveQueries", queryID)
	case http.StatusForbidden:
		return fmt.Errorf("the configured API key does not allow cancelling queries, use a key with write access")
	case http.StatusNotFound:
		return fmt.Errorf("no active query %q found: it may have already finished, or it was submitted by a different client", queryID)
	default:
		return fmt.Errorf("POST %s failed with status=%d %s", url, resp.StatusCode, http.StatusText(resp.StatusCode))
	}
}
