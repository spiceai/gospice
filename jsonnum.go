package gospice

import (
	"bytes"
	"encoding/json"
)

// decodeJSONExact decodes body into v, preserving JSON numbers that land in an
// interface-typed field as json.Number rather than converting them to float64.
//
// encoding/json decodes an untyped JSON number into float64, which holds only
// 53 bits of integer precision. Column values and primary keys reach this SDK
// through interface-backed maps, so an int64 or uint64 identifier beyond 2^53
// would be silently rounded on the way in: 9007199254740993 becomes
// 9007199254740992. That is worse than an error, because the value still looks
// like a plausible ID and quietly names the wrong row.
//
// json.Number keeps the number's original text, so callers can convert with the
// precision the column actually has - Int64, Uint64 via strconv, or Float64.
// Typed struct fields are unaffected and decode as they always did.
func decodeJSONExact(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	return dec.Decode(v)
}
