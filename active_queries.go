package gospice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	neturl "net/url"
	"time"
)

// ActiveQuery describes a synchronous query currently running on the runtime.
type ActiveQuery struct {
	// QueryID is assigned by the runtime and is what CancelActiveQuery takes.
	QueryID string `json:"query_id"`
	// Protocol the query arrived on: "http", "flight", "flightsql" or "internal".
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

// isUUID reports whether queryID has the shape the runtime parses as a UUID.
//
// The IDs this SDK cancels always come from ListActiveQueries, so a value that
// is not a UUID cannot name a running query.
func isUUID(queryID string) bool {
	if len(queryID) != 36 {
		return false
	}
	for i := 0; i < len(queryID); i++ {
		c := queryID[i]
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}

type activeQueriesResponse struct {
	Queries    []ActiveQuery `json:"queries"`
	TotalCount int           `json:"total_count"`
}

type cancelActiveQueryResponse struct {
	QueryID string `json:"query_id"`
	Status  string `json:"status"`
}

// ListActiveQueries returns the synchronous queries running in the caller's scope.
//
// Synchronous queries are the ones started by Sql, SqlWithParams, FlightSQL, NSQL and
// Search. Async query jobs are listed separately and are only available when the
// runtime runs in cluster mode.
//
// The runtime does not return a query's ID to the client that submitted it, so this is
// how to find the ID that CancelActiveQuery needs.
//
// Check your runtime version before relying on any scoping here. No runtime release
// up to and including v2.1.5 scopes these two endpoints at all: against one of those,
// this returns every active query the instance holds — including other principals'
// query IDs and SQL previews — to any caller with write access, and CancelActiveQuery
// will cancel any of them. Scoping landed in spiceai/spiceai#12841; see the package
// note on ListActiveQueries in README.md.
//
// On a runtime that does scope them, results are scoped to the authenticated
// principal — an API key or a client certificate — not to this SpiceClient: every
// client presenting the same credential lists the same queries, and requests for
// which the runtime establishes no principal share its public scope.
//
// Results also cover one runtime instance. The runtime holds active queries in memory
// per process, and this call addresses the client's HTTP endpoint — which
// WithHttpAddress sets, which defaults to Spice Cloud even when the Flight address is
// local, and which behind a load balancer may resolve to an instance that never
// received the query.
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
		return nil, fmt.Errorf("GET %s failed with status=%d %s: %s", url, resp.StatusCode, http.StatusText(resp.StatusCode), runtimeErrorMessage(resp))
	}

	var decoded activeQueriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("error decoding active queries response: %w", err)
	}

	return decoded.Queries, nil
}

// CancelActiveQuery cancels a running synchronous query by ID.
//
// queryID comes from ListActiveQueries. On a runtime that scopes these endpoints,
// cancellation is scoped to the authenticated principal, not to this SpiceClient: any
// client presenting the same credential can cancel the query, while an ID outside that
// scope is reported as not found. On a runtime that does not scope them — every release
// up to and including v2.1.5 — this cancels any active query on the instance,
// whichever principal submitted it. See ListActiveQueries.
//
// Like ListActiveQueries, this reaches one runtime instance: the client's HTTP endpoint.
//
// To cancel an async query job instead, use AsyncQuery.Cancel.
func (c *SpiceClient) CancelActiveQuery(ctx context.Context, queryID string) error {
	if queryID == "" {
		return fmt.Errorf("queryID is required, use ListActiveQueries to find one")
	}

	// queryID is caller input and reaches the runtime as a path segment. Reject
	// anything that is not a UUID here rather than building a path from it: "."
	// and ".." are unreserved, so escaping leaves them intact, and a proxy or
	// server that resolves dot segments would route this POST somewhere the
	// caller never named.
	if !isUUID(queryID) {
		return fmt.Errorf("query ID %q is not a valid UUID, use the QueryID from ListActiveQueries", queryID)
	}

	url := fmt.Sprintf("%s/v1/sql/%s/cancel", c.baseHttpUrl, neturl.PathEscape(queryID))

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
		return fmt.Errorf("no active query %q found: it may have already finished, or it was submitted under a different API key", queryID)
	default:
		return fmt.Errorf("POST %s failed with status=%d %s: %s", url, resp.StatusCode, http.StatusText(resp.StatusCode), runtimeErrorMessage(resp))
	}
}
