package gospice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

// Values that a float64 cannot hold exactly. 2^53+1 is the smallest positive
// integer float64 skips; the other two are realistic Snowflake/Twitter-style
// 64-bit IDs.
const (
	beyondFloat64Int  = int64(9007199254740993)
	largeInt64ID      = int64(9223372036854775807)
	largeUint64ID     = uint64(18446744073709551615)
	beyondFloat64Text = "9007199254740993"
)

func TestSearchPreservesLargeIntegers(t *testing.T) {
	// json.Unmarshal into an interface-backed map yields float64, which holds
	// 53 bits of integer precision: 9007199254740993 would come back as
	// ...992 and quietly name the wrong row.
	body := fmt.Sprintf(`{
		"results": [{
			"dataset": "app_messages",
			"_score": 0.5,
			"primary_key": {"id": %d},
			"data": {"account_id": %d, "counter": %d, "ratio": 0.25},
			"matches": {"body": [%d]},
			"metadata": {"shard": %d}
		}],
		"duration_ms": 4
	}`, beyondFloat64Int, largeInt64ID, largeUint64ID, beyondFloat64Int, beyondFloat64Int)

	spice := newSearchTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	})

	resp, err := spice.Search(context.Background(), &SearchRequest{Text: "tokyo"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("got %d results, want 1", len(resp.Results))
	}
	match := resp.Results[0]

	t.Run("primary key survives intact", func(t *testing.T) {
		num, ok := match.PrimaryKey["id"].(json.Number)
		if !ok {
			t.Fatalf("primary key id is %T, want json.Number", match.PrimaryKey["id"])
		}
		if num.String() != beyondFloat64Text {
			t.Errorf("id = %s, want %s", num.String(), beyondFloat64Text)
		}
		got, err := num.Int64()
		if err != nil {
			t.Fatalf("Int64: %v", err)
		}
		if got != beyondFloat64Int {
			t.Errorf("id = %d, want %d", got, beyondFloat64Int)
		}
	})

	t.Run("int64 max in an additional column", func(t *testing.T) {
		got, err := match.Data["account_id"].(json.Number).Int64()
		if err != nil {
			t.Fatalf("Int64: %v", err)
		}
		if got != largeInt64ID {
			t.Errorf("account_id = %d, want %d", got, largeInt64ID)
		}
	})

	t.Run("uint64 max in an additional column", func(t *testing.T) {
		var got uint64
		if _, err := fmt.Sscan(match.Data["counter"].(json.Number).String(), &got); err != nil {
			t.Fatalf("scanning uint64: %v", err)
		}
		if got != largeUint64ID {
			t.Errorf("counter = %d, want %d", got, largeUint64ID)
		}
	})

	t.Run("fractional values still convert", func(t *testing.T) {
		got, err := match.Data["ratio"].(json.Number).Float64()
		if err != nil {
			t.Fatalf("Float64: %v", err)
		}
		if got != 0.25 {
			t.Errorf("ratio = %v, want 0.25", got)
		}
	})

	t.Run("matches and metadata too", func(t *testing.T) {
		if got := match.Matches["body"][0].(json.Number).String(); got != beyondFloat64Text {
			t.Errorf("matches body = %s, want %s", got, beyondFloat64Text)
		}
		if got := match.Metadata["shard"].(json.Number).String(); got != beyondFloat64Text {
			t.Errorf("metadata shard = %s, want %s", got, beyondFloat64Text)
		}
	})

	t.Run("typed fields are unaffected", func(t *testing.T) {
		if match.Score != 0.5 {
			t.Errorf("Score = %v, want 0.5", match.Score)
		}
		if resp.DurationMs != 4 {
			t.Errorf("DurationMs = %d, want 4", resp.DurationMs)
		}
	})
}

func TestNsqlPreservesLargeIntegers(t *testing.T) {
	body := fmt.Sprintf(`{
		"sql": "SELECT id FROM t",
		"row_count": 1,
		"schema": {"fields": []},
		"data": [{"id": %d, "ratio": 0.5}]
	}`, beyondFloat64Int)

	spice := newNsqlTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	})

	resp, err := spice.Nsql(context.Background(), &NsqlRequest{Query: "how many?"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("got %d rows, want 1", len(resp.Data))
	}

	num, ok := resp.Data[0]["id"].(json.Number)
	if !ok {
		t.Fatalf("id is %T, want json.Number", resp.Data[0]["id"])
	}
	got, err := num.Int64()
	if err != nil {
		t.Fatalf("Int64: %v", err)
	}
	if got != beyondFloat64Int {
		t.Errorf("id = %d, want %d", got, beyondFloat64Int)
	}

	if resp.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1 (typed fields are unaffected)", resp.RowCount)
	}
}

func TestDecodeJSONExactRejectsMalformed(t *testing.T) {
	var into map[string]any
	if err := decodeJSONExact([]byte("not json"), &into); err == nil {
		t.Error("expected a decode error for malformed JSON")
	}
}
