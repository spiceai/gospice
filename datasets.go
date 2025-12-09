package gospice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type RefreshMode string

const (
	RefreshModeFull   RefreshMode = "full"
	RefreshModeAppend RefreshMode = "append"
)

type DatasetRefreshRequest struct {
	RefreshSQL *string      `json:"refresh_sql,omitempty"`
	Mode       *RefreshMode `json:"refresh_mode,omitempty"`
	MaxJitter  *string      `json:"refresh_jitter_max,omitempty"`
}

func (c *SpiceClient) RefreshDataset(ctx context.Context, dataset string, opts *DatasetRefreshRequest) error {
	var jsonData []byte
	var err error
	if opts == nil {
		jsonData = []byte("{}")
	} else {
		jsonData, err = json.Marshal(opts)
		if err != nil {
			return fmt.Errorf("error marshaling DatasetRefreshRequest opts: %w", err)
		}
	}

	body := bytes.NewBuffer(jsonData)
	url := fmt.Sprintf("%s/v1/datasets/%s/acceleration/refresh", c.baseHttpUrl, dataset)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req = req.WithContext(c.traceHttpRequest(ctx, "RefreshDataset", req))

	req.Header.Set("X-API-Key", c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("user-agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			// Log error but don't fail the function
			log.Printf("warning: failed to close response body: %v", closeErr)
		}
	}()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("POST %s failed with status=%d. body=%v", url, resp.StatusCode, body)
	}

	return nil
}
