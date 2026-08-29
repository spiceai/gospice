package gospice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"unicode/utf8"
)

// maxErrorMessageBytes bounds how much of an error response reaches the caller.
//
// WithHttpAddress accepts any HTTP address, so the body of a failed request is
// not necessarily a runtime diagnostic — an arbitrarily large one would be
// buffered and then copied into the returned error string. A runtime
// explanation is a sentence; this is far above one and far below a size worth
// holding twice in memory.
const maxErrorMessageBytes = 64 << 10

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
	// One byte past the bound, so a body that filled it exactly is
	// distinguishable from one that was cut short.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorMessageBytes+1))
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
// whether the request succeeded — which is also why the bound is applied here
// rather than only at the read: those callers arrive with a body no one capped.
func errorMessageFromBody(body []byte) string {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return "(no response body)"
	}

	var errResp runtimeErrorResponse
	if json.Unmarshal(trimmed, &errResp) == nil && errResp.Error != "" {
		return truncateErrorMessage(errResp.Error)
	}

	return truncateErrorMessage(string(trimmed))
}

// truncateErrorMessage bounds a message at maxErrorMessageBytes, saying so when
// it cuts. A silently shortened explanation is worse than a long one: the reader
// would take a sentence that stops mid-word for what the runtime said.
func truncateErrorMessage(message string) string {
	if len(message) <= maxErrorMessageBytes {
		return message
	}

	// Back off to a rune boundary: cutting mid-rune would put invalid UTF-8 in
	// the error string, which formats as a replacement character rather than as
	// the text the runtime sent.
	cut := maxErrorMessageBytes
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}

	return fmt.Sprintf("%s… (truncated at %d bytes)", message[:cut], maxErrorMessageBytes)
}
