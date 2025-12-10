package gospice

import (
	"context"
	"fmt"
	"net/http"
)

// IsSpiceHealthy checks if the Spice instance is healthy by calling the /health endpoint.
// This is an unauthenticated endpoint that returns true if the Spice instance is running.
func (c *SpiceClient) IsSpiceHealthy(ctx context.Context) bool {
	url := fmt.Sprintf("%s/health", c.baseHttpUrl)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}

	req = req.WithContext(c.traceHttpRequest(ctx, "IsSpiceHealthy", req))
	req.Header.Set("user-agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// IsSpiceReady checks if the Spice instance is ready by calling the /v1/ready endpoint.
// This is an unauthenticated endpoint that returns true if the Spice instance is ready to serve queries.
func (c *SpiceClient) IsSpiceReady(ctx context.Context) bool {
	url := fmt.Sprintf("%s/v1/ready", c.baseHttpUrl)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}

	req = req.WithContext(c.traceHttpRequest(ctx, "IsSpiceReady", req))
	req.Header.Set("user-agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}
