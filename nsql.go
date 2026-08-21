package gospice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// nsqlJSONMediaType asks the runtime for the envelope carrying the generated
// SQL alongside the results. Without it /v1/nsql returns a bare array of rows
// and the generated SQL is lost.
const nsqlJSONMediaType = "application/vnd.spiceai.nsql.v1+json"

// nsqlSQLMediaType asks the runtime to generate SQL without executing it.
const nsqlSQLMediaType = "application/sql"

// NsqlRequest describes a natural-language query against the runtime's
// /v1/nsql endpoint.
//
// Only Query is required. The runtime needs an LLM model configured in the
// Spicepod to translate it; when exactly one is configured, Model may be left
// empty and the runtime selects it.
type NsqlRequest struct {
	// Query is the question to answer, in natural language. Required.
	Query string `json:"query"`

	// Model names the LLM used to generate SQL. When empty, the runtime uses
	// the only compatible model configured in the Spicepod, and reports an
	// error if there is not exactly one.
	Model string `json:"model,omitempty"`

	// Datasets hints which datasets to sample when building model context.
	// This is a sampling hint only - it does not restrict which tables the
	// generated query may reference. When empty, all datasets are used.
	Datasets []string `json:"datasets,omitempty"`

	// SampleDataEnabled includes sample rows in the context given to the
	// model. It improves generation on ambiguous schemas at the cost of
	// sending data values to the model.
	SampleDataEnabled bool `json:"sample_data_enabled,omitempty"`

	// PromptCacheKey is a stable key forwarded to the model provider for
	// prompt caching. Reuse it across related requests to benefit from it.
	PromptCacheKey string `json:"prompt_cache_key,omitempty"`
}

// NsqlField describes one column of an NsqlResponse.
type NsqlField struct {
	// Name is the column name.
	Name string `json:"name"`

	// DataType is the column's Arrow type in its JSON encoding. Simple types
	// encode as a quoted string ("Utf8", "Int64"); parameterized ones as an
	// object (for example {"Timestamp":["Nanosecond",null]}).
	DataType json.RawMessage `json:"data_type"`

	// Nullable reports whether the column admits nulls.
	Nullable bool `json:"nullable"`
}

// NsqlSchema is the schema of the rows an Nsql call returned.
//
// Fields is empty when the generated query returned no rows - the runtime
// omits the schema body in that case.
type NsqlSchema struct {
	Fields []NsqlField `json:"fields"`
}

// NsqlResponse is the result of running a natural-language query.
type NsqlResponse struct {
	// SQL is the query the model generated. It is worth logging: a surprising
	// result is usually a surprising query.
	SQL string `json:"sql"`

	// RowCount is the number of rows returned.
	RowCount int `json:"row_count"`

	// Schema describes the columns in Data.
	Schema NsqlSchema `json:"schema"`

	// Data holds the rows, each keyed by column name. Values are decoded from
	// JSON, so they carry JSON's types rather than the Arrow types named in
	// Schema - numbers arrive as json.Number, which keeps the value's original
	// text so a 64-bit identifier is not rounded to float64's 53 bits. Convert
	// with Int64, Float64, or String as the column requires. Use
	// NsqlGenerateSQL with Query when Arrow-typed results matter.
	Data []map[string]any `json:"data"`
}

// Nsql answers req.Query by having the runtime's configured LLM generate SQL,
// then running it.
//
// The generated SQL is returned in NsqlResponse.SQL. The runtime executes it
// read-only and retries generation when the query fails to run, so a returned
// error means generation or execution failed repeatedly.
//
// Nsql requires an LLM model in the Spicepod. See
// https://docs.spice.ai/features/text-to-sql for how to configure one.
func (c *SpiceClient) Nsql(ctx context.Context, req *NsqlRequest) (*NsqlResponse, error) {
	respBody, err := c.doNsqlRequest(ctx, req, "Nsql", nsqlJSONMediaType)
	if err != nil {
		return nil, err
	}

	var nsqlResp NsqlResponse
	if err := decodeJSONExact(respBody, &nsqlResp); err != nil {
		return nil, fmt.Errorf("error decoding response from POST %s/v1/nsql: %w", c.baseHttpUrl, err)
	}

	return &nsqlResp, nil
}

// NsqlGenerateSQL translates req.Query into SQL without running it.
//
// Use it to inspect or edit the query before running it, or to run it through
// Query or Sql so the results arrive as Arrow rather than decoded JSON.
func (c *SpiceClient) NsqlGenerateSQL(ctx context.Context, req *NsqlRequest) (string, error) {
	respBody, err := c.doNsqlRequest(ctx, req, "NsqlGenerateSQL", nsqlSQLMediaType)
	if err != nil {
		return "", err
	}

	return string(bytes.TrimSpace(respBody)), nil
}

// doNsqlRequest posts req to /v1/nsql asking for accept, and returns the
// response body when the runtime answered 200.
func (c *SpiceClient) doNsqlRequest(ctx context.Context, req *NsqlRequest, operation string, accept string) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("req is required")
	}
	if req.Query == "" {
		return nil, fmt.Errorf("req.Query is required and must be a non-empty natural language query")
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshaling NsqlRequest: %w", err)
	}

	url := fmt.Sprintf("%s/v1/nsql", c.baseHttpUrl)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	httpReq = httpReq.WithContext(c.traceHttpRequest(ctx, operation, httpReq))

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", accept)
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
		// The runtime explains NSQL failures in a plain-text body - a missing
		// or ambiguous model, or SQL that would not run. Surface it rather
		// than only the status code.
		return nil, fmt.Errorf("POST %s failed with status=%d: %s", url, resp.StatusCode, bytes.TrimSpace(respBody))
	}

	return respBody, nil
}
