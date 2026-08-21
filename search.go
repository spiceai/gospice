package gospice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// SearchRequest describes a search against the runtime's /v1/search endpoint.
//
// Only Text is required. Supplying Keywords adds a lexical pass, which the
// runtime combines with the vector scores into a single hybrid ranking.
type SearchRequest struct {
	// Text is the text to find similar documents for. Required.
	Text string `json:"text"`

	// Datasets restricts the search to the named datasets. When empty, the
	// runtime searches every searchable dataset.
	Datasets []string `json:"datasets,omitempty"`

	// Limit caps the number of matches returned per dataset. When nil, the
	// runtime applies its own default.
	Limit *int `json:"limit,omitempty"`

	// Where is a SQL predicate filtering candidate rows, without the leading
	// WHERE - for example "user_id = 42".
	Where *string `json:"where,omitempty"`

	// AdditionalColumns names extra columns to return with each match. A
	// primary key column is returned in SearchMatch.PrimaryKey, the rest in
	// SearchMatch.Data.
	AdditionalColumns []string `json:"additional_columns,omitempty"`

	// Keywords drives the lexical pass of a hybrid search.
	Keywords []string `json:"keywords,omitempty"`
}

// SearchMatch is a single document matched by Search.
//
// The runtime omits primary_key, data and metadata from a match that has
// none, so PrimaryKey, Data and Metadata are nil rather than empty in that
// case. Reading from a nil map is safe and reports no entries; assigning into
// one panics, so allocate before writing.
type SearchMatch struct {
	// Dataset is the dataset the match was found in.
	Dataset string `json:"dataset"`

	// Score is the match's similarity to the query. Higher is more similar.
	Score float64 `json:"_score"`

	// Matches holds the matched values keyed by the column they came from.
	// Each value is a slice because one column can contribute several chunks
	// to a single match.
	Matches map[string][]any `json:"matches"`

	// PrimaryKey identifies the matched row. Nil when the dataset declares no
	// primary key.
	PrimaryKey map[string]any `json:"primary_key"`

	// Data holds any AdditionalColumns that were requested. Nil when none were
	// requested.
	Data map[string]any `json:"data"`

	// Metadata holds extra per-match metadata the runtime attached. Nil when
	// it attached none.
	Metadata map[string]any `json:"metadata"`
}

// SearchResponse is the result of a single Search call.
type SearchResponse struct {
	// Results are the matches, ordered by descending score.
	Results []SearchMatch `json:"results"`

	// DurationMs is how long the runtime reported the search took.
	DurationMs uint64 `json:"duration_ms"`
}

// searchErrorResponse is the runtime's JSON error body for a failed search.
type searchErrorResponse struct {
	Error string `json:"error"`
}

// searchErrorMessage extracts the message to report from a failed search response.
//
// The runtime answers some failures with a JSON {"error": "..."} body and others —
// "No data sources provided", for instance — with plain text, so both shapes have to
// be handled or the part that tells the caller what to fix is lost.
func searchErrorMessage(body []byte) string {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return "(no response body)"
	}

	var errResp searchErrorResponse
	if json.Unmarshal(trimmed, &errResp) == nil && errResp.Error != "" {
		return errResp.Error
	}

	return string(trimmed)
}

// Search finds documents similar to req.Text by calling the runtime's
// /v1/search endpoint.
//
// It runs against datasets that have an embedding column and a loaded
// embedding model. See https://docs.spice.ai/features/search-and-retrieval for
// how to configure them.
func (c *SpiceClient) Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("req is required")
	}
	if req.Text == "" {
		return nil, fmt.Errorf("req.Text is required and must be a non-empty search string")
	}
	if req.Limit != nil && *req.Limit < 1 {
		return nil, fmt.Errorf("req.Limit must be greater than 0, got %d", *req.Limit)
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshaling SearchRequest: %w", err)
	}

	url := fmt.Sprintf("%s/v1/search", c.baseHttpUrl)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	httpReq = httpReq.WithContext(c.traceHttpRequest(ctx, "Search", httpReq))

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("user-agent", c.userAgent)
	// Only send the key when there is one — an empty X-API-Key reads as a
	// supplied-but-invalid credential to auth middleware, which is different
	// from omitting the header. Matches IsSpiceReady in client.go.
	if c.apiKey != "" {
		httpReq.Header.Set("X-API-Key", c.apiKey)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response from POST %s: %w", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("POST %s failed with status=%d: %s", url, resp.StatusCode, searchErrorMessage(respBody))
	}

	var searchResp SearchResponse
	if err := json.Unmarshal(respBody, &searchResp); err != nil {
		return nil, fmt.Errorf("error decoding response from POST %s: %w", url, err)
	}

	return &searchResp, nil
}
