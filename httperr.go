package gospice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// runtimeErrorResponse is the runtime's JSON error body for a failed request.
type runtimeErrorResponse struct {
	Error string `json:"error"`
}

// runtimeErrorMessage reads an error response's body and returns the runtime's
// explanation of the failure.
//
// Reporting only the status code discards the one part of the response that
// says what to fix: a 404 from a dataset refresh means nothing on its own, while
// the body names the dataset the runtime could not find. Reading a body is
// itself I/O, so a read that fails says so rather than being reported as an
// absent explanation.
//
// The caller is expected to have already classified the status as an error, and
// still owns closing resp.Body.
func runtimeErrorMessage(resp *http.Response) string {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf("(error body could not be read: %v)", err)
	}

	return errorMessageFromBody(body)
}

// errorMessageFromBody interprets an already-read error response body.
//
// The runtime explains some failures as JSON ({"error": "..."}) and others as
// plain text, so try the JSON shape and fall back to the raw body. Separate from
// runtimeErrorMessage for the callers that must read the body before knowing
// whether the request succeeded.
func errorMessageFromBody(body []byte) string {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return "(no response body)"
	}

	var errResp runtimeErrorResponse
	if json.Unmarshal(trimmed, &errResp) == nil && errResp.Error != "" {
		return errResp.Error
	}

	return string(trimmed)
}
