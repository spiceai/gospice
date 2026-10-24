package gospice

import (
	"context"
	"database/sql"
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// recordCapture keeps the bound parameter batch so tests can read the Arrow value.
type recordCapture struct {
	adbc.Statement
	rec arrow.RecordBatch
}

func (s *recordCapture) Bind(_ context.Context, rec arrow.RecordBatch) error {
	rec.Retain()
	s.rec = rec
	return nil
}

func bindRecord(t *testing.T, params ...any) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	stmt := &recordCapture{}
	if err := (&SpiceClient{}).bindParameters(&ADBCClient{mem: mem}, stmt, params...); err != nil {
		t.Fatalf("bindParameters(%v) failed: %v", params, err)
	}
	t.Cleanup(func() { stmt.rec.Release() })
	return stmt.rec
}

func decimal128LE(n decimal128.Num) [16]byte {
	var raw [16]byte
	binary.LittleEndian.PutUint64(raw[0:8], n.LowBits())
	binary.LittleEndian.PutUint64(raw[8:16], uint64(n.HighBits()))
	return raw
}

func decimal256LE(n decimal256.Num) [32]byte {
	arr := n.Array()
	var raw [32]byte
	binary.LittleEndian.PutUint64(raw[0:8], arr[0])
	binary.LittleEndian.PutUint64(raw[8:16], arr[1])
	binary.LittleEndian.PutUint64(raw[16:24], arr[2])
	binary.LittleEndian.PutUint64(raw[24:32], arr[3])
	return raw
}

func mustDecimal128(t *testing.T, s string, precision, scale int32) decimal128.Num {
	t.Helper()
	n, err := decimal128.FromString(s, precision, scale)
	if err != nil {
		t.Fatalf("decimal128.FromString(%q, %d, %d): %v", s, precision, scale, err)
	}
	return n
}

func mustDecimal256(t *testing.T, s string, precision, scale int32) decimal256.Num {
	t.Helper()
	n, err := decimal256.FromString(s, precision, scale)
	if err != nil {
		t.Fatalf("decimal256.FromString(%q, %d, %d): %v", s, precision, scale, err)
	}
	return n
}

func TestDecimalParamRoundTrip(t *testing.T) {
	type dec128Case struct {
		name      string
		text      string
		precision int32
		scale     int32
	}
	dec128Cases := []dec128Case{
		{name: "positive", text: "1.0000000001", precision: 38, scale: 10},
		{name: "negative", text: "-1.5", precision: 38, scale: 10},
		{name: "scale-0", text: "42", precision: 38, scale: 0},
		{name: "max-precision", text: "99999999999999999999999999999999999999", precision: 38, scale: 0},
	}

	for _, tc := range dec128Cases {
		want := mustDecimal128(t, tc.text, tc.precision, tc.scale)
		wantType := &arrow.Decimal128Type{Precision: tc.precision, Scale: tc.scale}

		t.Run("128/"+tc.name+"/bytes", func(t *testing.T) {
			rec := bindRecord(t, Decimal128Param(decimal128LE(want), tc.precision, tc.scale))
			gotType := rec.Schema().Field(0).Type
			if !arrow.TypeEqual(gotType, wantType) {
				t.Fatalf("type = %v, want %v", gotType, wantType)
			}
			got := rec.Column(0).(*array.Decimal128).Value(0)
			if got != want {
				t.Fatalf("value = %s, want %s", got.ToString(tc.scale), want.ToString(tc.scale))
			}
		})

		t.Run("128/"+tc.name+"/num", func(t *testing.T) {
			rec := bindRecord(t, DecimalParam(want, tc.precision, tc.scale))
			got := rec.Column(0).(*array.Decimal128).Value(0)
			if got != want {
				t.Fatalf("value = %s, want %s", got.ToString(tc.scale), want.ToString(tc.scale))
			}
		})

		t.Run("128/"+tc.name+"/inferred-num", func(t *testing.T) {
			rec := bindRecord(t, want)
			gotType := rec.Schema().Field(0).Type
			// Inference uses the default (38, 10); scale-0 / max-precision still
			// store the same coefficient, so the Num must match even if scale differs.
			if gotType.ID() != arrow.DECIMAL128 {
				t.Fatalf("type = %v, want decimal128", gotType)
			}
			got := rec.Column(0).(*array.Decimal128).Value(0)
			if got != want {
				t.Fatalf("value = %v, want %v", got, want)
			}
		})
	}

	type dec256Case struct {
		name      string
		text      string
		precision int32
		scale     int32
	}
	dec256Cases := []dec256Case{
		{name: "positive", text: "1.0000000001", precision: 76, scale: 10},
		{name: "negative", text: "-1.5", precision: 76, scale: 10},
		{name: "scale-0", text: "42", precision: 76, scale: 0},
		{name: "max-precision", text: "9999999999999999999999999999999999999999999999999999999999999999999999999999", precision: 76, scale: 0},
	}

	for _, tc := range dec256Cases {
		want := mustDecimal256(t, tc.text, tc.precision, tc.scale)
		wantType := &arrow.Decimal256Type{Precision: tc.precision, Scale: tc.scale}

		t.Run("256/"+tc.name+"/bytes", func(t *testing.T) {
			rec := bindRecord(t, Decimal256Param(decimal256LE(want), tc.precision, tc.scale))
			gotType := rec.Schema().Field(0).Type
			if !arrow.TypeEqual(gotType, wantType) {
				t.Fatalf("type = %v, want %v", gotType, wantType)
			}
			got := rec.Column(0).(*array.Decimal256).Value(0)
			if got != want {
				t.Fatalf("value = %s, want %s", got.ToString(tc.scale), want.ToString(tc.scale))
			}
		})

		t.Run("256/"+tc.name+"/num", func(t *testing.T) {
			rec := bindRecord(t, DecimalParam(want, tc.precision, tc.scale))
			got := rec.Column(0).(*array.Decimal256).Value(0)
			if got != want {
				t.Fatalf("value = %s, want %s", got.ToString(tc.scale), want.ToString(tc.scale))
			}
		})

		t.Run("256/"+tc.name+"/inferred-num", func(t *testing.T) {
			rec := bindRecord(t, want)
			if rec.Schema().Field(0).Type.ID() != arrow.DECIMAL256 {
				t.Fatalf("type = %v, want decimal256", rec.Schema().Field(0).Type)
			}
			got := rec.Column(0).(*array.Decimal256).Value(0)
			if got != want {
				t.Fatalf("value = %v, want %v", got, want)
			}
		})
	}
}

func TestDecimalParamNewTypedParam(t *testing.T) {
	n := mustDecimal128(t, "1.0000000001", 38, 10)
	rec := bindRecord(t, NewTypedParam(n, &arrow.Decimal128Type{Precision: 38, Scale: 10}))
	got := rec.Column(0).(*array.Decimal128).Value(0)
	if got != n {
		t.Fatalf("NewTypedParam(decimal128.Num) value = %s, want %s", got.ToString(10), n.ToString(10))
	}

	n256 := mustDecimal256(t, "-1.5", 76, 10)
	rec = bindRecord(t, NewParam(n256))
	got256 := rec.Column(0).(*array.Decimal256).Value(0)
	if got256 != n256 {
		t.Fatalf("NewParam(decimal256.Num) value = %s, want %s", got256.ToString(10), n256.ToString(10))
	}
}

func TestDecimalParamPreservesScaleOnWrappers(t *testing.T) {
	n := decimal128.FromI64(42)
	wantType := &arrow.Decimal128Type{Precision: 38, Scale: 0}

	t.Run("pointer", func(t *testing.T) {
		rec := bindRecord(t, DecimalParam(&n, 38, 0))
		if !arrow.TypeEqual(rec.Schema().Field(0).Type, wantType) {
			t.Fatalf("type = %v, want %v", rec.Schema().Field(0).Type, wantType)
		}
		got := rec.Column(0).(*array.Decimal128).Value(0)
		if got != n {
			t.Fatalf("value = %s, want %s", got.ToString(0), n.ToString(0))
		}
	})

	t.Run("sql.Null valid", func(t *testing.T) {
		rec := bindRecord(t, DecimalParam(sql.Null[decimal128.Num]{V: n, Valid: true}, 38, 0))
		if !arrow.TypeEqual(rec.Schema().Field(0).Type, wantType) {
			t.Fatalf("type = %v, want %v", rec.Schema().Field(0).Type, wantType)
		}
		got := rec.Column(0).(*array.Decimal128).Value(0)
		if got != n {
			t.Fatalf("value = %s, want %s", got.ToString(0), n.ToString(0))
		}
	})

	t.Run("sql.Null invalid", func(t *testing.T) {
		rec := bindRecord(t, DecimalParam(sql.Null[decimal128.Num]{}, 38, 0))
		if !arrow.TypeEqual(rec.Schema().Field(0).Type, wantType) {
			t.Fatalf("type = %v, want %v", rec.Schema().Field(0).Type, wantType)
		}
		if !rec.Column(0).IsNull(0) {
			t.Fatal("expected a NULL decimal(38, 0)")
		}
	})

	t.Run("pointer to pointer", func(t *testing.T) {
		p := &n
		rec := bindRecord(t, DecimalParam(&p, 38, 0))
		if !arrow.TypeEqual(rec.Schema().Field(0).Type, wantType) {
			t.Fatalf("type = %v, want %v", rec.Schema().Field(0).Type, wantType)
		}
		got := rec.Column(0).(*array.Decimal128).Value(0)
		if got != n {
			t.Fatalf("value = %s, want %s", got.ToString(0), n.ToString(0))
		}
	})
}

func TestDecimalNumWidthBoundsRecursivePointer(t *testing.T) {
	type P *P
	if got := decimalNumWidth(P(nil)); got != 0 {
		t.Fatalf("decimalNumWidth(recursive nil pointer) = %d, want 0", got)
	}
	// Must return, not hang: type P *P makes Elem() the same pointer type.
	_ = DecimalParam(P(nil), 38, 0)
}

func TestInferArrowTypeDecimalNum(t *testing.T) {
	n128 := mustDecimal128(t, "1.0000000001", 38, 10)
	dt, err := inferArrowType(n128)
	if err != nil {
		t.Fatalf("inferArrowType(decimal128.Num): %v", err)
	}
	want128 := &arrow.Decimal128Type{Precision: 38, Scale: 10}
	if !arrow.TypeEqual(dt, want128) {
		t.Fatalf("inferArrowType(decimal128.Num) = %v, want %v", dt, want128)
	}

	n256 := mustDecimal256(t, "1.0000000001", 76, 10)
	dt, err = inferArrowType(n256)
	if err != nil {
		t.Fatalf("inferArrowType(decimal256.Num): %v", err)
	}
	want256 := &arrow.Decimal256Type{Precision: 76, Scale: 10}
	if !arrow.TypeEqual(dt, want256) {
		t.Fatalf("inferArrowType(decimal256.Num) = %v, want %v", dt, want256)
	}
}
