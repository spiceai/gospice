package gospice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// ComponentStatus is the state of a single runtime component.
type ComponentStatus string

const (
	ComponentStatusInitializing ComponentStatus = "Initializing"
	ComponentStatusReady        ComponentStatus = "Ready"
	ComponentStatusDisabled     ComponentStatus = "Disabled"
	ComponentStatusError        ComponentStatus = "Error"
	ComponentStatusRefreshing   ComponentStatus = "Refreshing"
	ComponentStatusShuttingDown ComponentStatus = "ShuttingDown"
	ComponentStatusNotLoaded    ComponentStatus = "NotLoaded"
)

// ConnectionDetails describes the status of one runtime connection.
type ConnectionDetails struct {
	// Name of the connection, one of "http", "flight", "metrics" or "opentelemetry".
	Name string `json:"name"`
	// Endpoint the connection is served on, or "N/A" when the component is disabled.
	Endpoint string `json:"endpoint"`
	// Status of the component.
	Status ComponentStatus `json:"status"`
}

// IsReady reports whether the component is ready to accept connections.
func (d ConnectionDetails) IsReady() bool {
	return d.Status == ComponentStatusReady
}

// RuntimeStatus returns the status of each runtime connection.
//
// Unlike IsSpiceReady, which reports a single boolean for the whole runtime, this
// reports per-component state and so can distinguish a runtime that is still
// initializing from one whose Flight endpoint is failing.
func (c *SpiceClient) RuntimeStatus(ctx context.Context) ([]ConnectionDetails, error) {
	url := fmt.Sprintf("%s/v1/status", c.baseHttpUrl)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req = req.WithContext(c.traceHttpRequest(ctx, "RuntimeStatus", req))

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

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s failed with status=%d %s: %s", url, resp.StatusCode, http.StatusText(resp.StatusCode), runtimeErrorMessage(resp))
	}

	var details []ConnectionDetails
	if err := json.NewDecoder(resp.Body).Decode(&details); err != nil {
		return nil, fmt.Errorf("error decoding runtime status response: %w", err)
	}

	return details, nil
}
