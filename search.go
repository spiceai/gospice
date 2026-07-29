package gospice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// SearchRequest describes a search against one or more datasets.
//
// Text is required. Every other field is optional: when Datasets is empty the
// runtime searches every dataset that has an embedding column, and when Limit
// is zero the runtime applies its own default.
type SearchRequest struct {
	// Text is the query to find similar documents for.
	Text string `json:"text"`

	// Datasets restricts the search to the named datasets. Empty means every
	// dataset with an embedding column and a loaded embedding model.
	Datasets []string `json:"datasets,omitempty"`

	// Limit caps the number of matches returned per dataset.
	Limit int `json:"limit,omitempty"`

	// Where is an SQL predicate applied before the search, without the WHERE
	// keyword — for example `city = 'Tokyo'`.
	Where string `json:"where,omitempty"`

	// AdditionalColumns names extra dataset columns to return. A column that is
	// part of the dataset's primary key is returned in SearchMatch.PrimaryKey
	// rather than SearchMatch.Data.
	AdditionalColumns []string `json:"additional_columns,omitempty"`

	// Keywords pre-filters the embedding column with a lexical search before the
	// vector search runs, producing a hybrid search.
	Keywords []string `json:"keywords,omitempty"`
}

// SearchMatch is a single document matched by a search.
type SearchMatch struct {
	// Dataset is the dataset the match was found in.
	Dataset string `json:"dataset"`

	// Score is the similarity of the match to the query text. Higher is closer.
	Score float64 `json:"_score"`

	// Matches holds the matched values of each searched column.
	Matches map[string][]any `json:"matches,omitempty"`

	// PrimaryKey identifies the matched row. Empty unless the dataset declares a
	// primary key.
	PrimaryKey map[string]any `json:"primary_key,omitempty"`

	// Data holds the columns requested via SearchRequest.AdditionalColumns.
	Data map[string]any `json:"data,omitempty"`

	// Metadata holds any additional metadata the runtime attached to the match.
	Metadata map[string]any `json:"metadata,omitempty"`
}

// SearchResponse is the result of a search.
type SearchResponse struct {
	// Results are the matches, ordered by descending score.
	Results []SearchMatch `json:"results"`

	// DurationMs is how long the runtime took to run the search.
	DurationMs int64 `json:"duration_ms"`
}

// searchErrorResponse is the runtime's JSON error body for a failed search.
type searchErrorResponse struct {
	Error string `json:"error"`
}

// searchErrorMessage extracts the message to report from a failed search response.
//
// The runtime answers some failures with a JSON {"error": "..."} body and others —
// "Search cannot be run on X because it has no embeddings or full text search
// indexes", for instance — with plain text, so both shapes have to be handled or the
// part that tells the caller what to fix is lost.
func searchErrorMessage(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return "(no response body)"
	}

	var errResp searchErrorResponse
	if json.Unmarshal(body, &errResp) == nil && errResp.Error != "" {
		return errResp.Error
	}

	return trimmed
}

// Search runs a vector similarity, keyword, or hybrid search against datasets
// that have an embedding column and a loaded embedding model.
//
// Set SearchRequest.Keywords to pre-filter with a lexical search before the
// vector search, which makes the search hybrid.
//
//	resp, err := spice.Search(ctx, &gospice.SearchRequest{
//		Text:     "tickets to Tokyo",
//		Datasets: []string{"app_messages"},
//		Limit:    3,
//	})
//	if err != nil {
//		return err
//	}
//	for _, match := range resp.Results {
//		fmt.Println(match.Dataset, match.Score, match.Matches)
//	}
func (c *SpiceClient) Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("search request is required")
	}
	if req.Text == "" {
		return nil, fmt.Errorf("search request Text is required")
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
		return nil, fmt.Errorf("error reading search response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("search failed with status=%d: %s", resp.StatusCode, searchErrorMessage(respBody))
	}

	var searchResp SearchResponse
	if err := json.Unmarshal(respBody, &searchResp); err != nil {
		return nil, fmt.Errorf("error parsing search response: %w", err)
	}

	return &searchResp, nil
}
