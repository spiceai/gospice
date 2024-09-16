package gospice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type RefreshMode string

const (
	RefreshModeFull   RefreshMode = "full"
	RefreshModeAppend RefreshMode = "append"
)

type DatasetRefreshApiRequest struct {
	RefreshSQL *string      `json:"refresh_sql,omitempty"`
	Mode       *RefreshMode `json:"refresh_mode,omitempty"`
	MaxJitter  *string      `json:"refresh_jitter_max,omitempty"`
}

func constructRefreshRequest(sql *string, mode *RefreshMode, max_jitter *string) (io.Reader, error) {
	r := DatasetRefreshApiRequest{}
	if sql == nil && mode == nil && max_jitter == nil {
		return nil, nil
	}
	if sql != nil {
		r.RefreshSQL = sql
	}
	if mode != nil {
		r.Mode = mode
	}
	if max_jitter != nil {
		r.MaxJitter = max_jitter
	}
	jsonData, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	body := bytes.NewBuffer(jsonData)
	return body, nil
}

func (c *SpiceClient) RefreshDataset(ctx context.Context, dataset string, refresh_sql *string, refresh_mode *RefreshMode, max_jitter *string) error {
	body, err := constructRefreshRequest(refresh_sql, refresh_mode, max_jitter)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/v1/datasets/%s/acceleration/refresh", c.baseHttpUrl, dataset)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("X-API-Key", c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", fmt.Sprintf("gospice %s", GO_SPICE_VERSION))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("POST %s failed with status=%d. body=%v", url, resp.StatusCode, body)
	}

	return nil
}
