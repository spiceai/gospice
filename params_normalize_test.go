package gospice

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// bindCapture is an adbc.Statement whose Bind records the parameter batch.
type bindCapture struct {
	adbc.Statement
	fields []arrow.Field
	values []any
	nulls  []bool
}

func (s *bindCapture) Bind(_ context.Context, rec arrow.RecordBatch) error {
	s.fields = rec.Schema().Fields()
	for _, col := range rec.Columns() {
		s.nulls = append(s.nulls, col.IsNull(0))
		s.values = append(s.values, col.GetOneForMarshal(0))
	}
	return nil
}

func bindForTest(t *testing.T, params ...any) *bindCapture {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	stmt := &bindCapture{}
	if err := (&SpiceClient{}).bindParameters(&ADBCClient{mem: mem}, stmt, params...); err != nil {
		t.Fatalf("bindParameters(%v) failed: %v", params, err)
	}
	return stmt
}

type vendorID int64
type tripLabel string
type rawPayload []byte

// upperValuer is a driver.Valuer that is not a database/sql type.
type upperValuer string

func (u upperValuer) Value() (driver.Value, error) { return strings.ToUpper(string(u)), nil }

// ptrValuer implements driver.Valuer only on its pointer receiver.
type ptrValuer int64

func (v *ptrValuer) Value() (driver.Value, error) { return fmt.Sprintf("pv-%d", int64(*v)), nil }

// namedByte is a byte whose slice type is not convertible to []byte.
type namedByte byte
type namedBytes []namedByte

type failingValuer struct{}

func (failingValuer) Value() (driver.Value, error) { return nil, errors.New("no value") }

// selfValuer returns itself, which would loop forever without a depth bound.
type selfValuer struct{}

func (s selfValuer) Value() (driver.Value, error) { return s, nil }

func TestBindParametersAcceptsNullableAndNamedValues(t *testing.T) {
	n := int64(7)
	name := "vendor"
	pickup := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	var nilInt *int64
	var nilTime *time.Time
	var nilNull *sql.NullInt64
	pv := ptrValuer(5)

	tests := []struct {
		name     string
		value    any
		wantType arrow.DataType
		wantNull bool
		want     any
	}{
		{"*int64", &n, arrow.PrimitiveTypes.Int64, false, int64(7)},
		{"**string", func() **string { p := &name; return &p }(), arrow.BinaryTypes.String, false, "vendor"},
		{"nil *int64", nilInt, arrow.PrimitiveTypes.Int64, true, nil},
		{"nil *time.Time", nilTime, arrow.FixedWidthTypes.Timestamp_ns, true, nil},
		{"nil *sql.NullInt64", nilNull, arrow.PrimitiveTypes.Int64, true, nil},
		{"*time.Time", &pickup, arrow.FixedWidthTypes.Timestamp_ns, false, "2024-01-31T00:00:00Z"},
		{"named int64", vendorID(2), arrow.PrimitiveTypes.Int64, false, int64(2)},
		{"named string", tripLabel("airport"), arrow.BinaryTypes.String, false, "airport"},
		{"named []byte", rawPayload("ab"), arrow.BinaryTypes.Binary, false, []byte("ab")},
		{"sql.NullInt64 valid", sql.NullInt64{Int64: 3, Valid: true}, arrow.PrimitiveTypes.Int64, false, int64(3)},
		{"sql.NullInt64 null", sql.NullInt64{}, arrow.PrimitiveTypes.Int64, true, nil},
		{"sql.NullString null", sql.NullString{}, arrow.BinaryTypes.String, true, nil},
		{"sql.NullInt32 null", sql.NullInt32{}, arrow.PrimitiveTypes.Int32, true, nil},
		{"sql.NullInt16 null", sql.NullInt16{}, arrow.PrimitiveTypes.Int16, true, nil},
		{"sql.NullByte valid", sql.NullByte{Byte: 9, Valid: true}, arrow.PrimitiveTypes.Uint8, false, uint8(9)},
		{"sql.NullFloat64 valid", sql.NullFloat64{Float64: 1.5, Valid: true}, arrow.PrimitiveTypes.Float64, false, 1.5},
		{"sql.NullBool null", sql.NullBool{}, arrow.FixedWidthTypes.Boolean, true, nil},
		{"sql.NullTime valid", sql.NullTime{Time: pickup, Valid: true}, arrow.FixedWidthTypes.Timestamp_ns, false, "2024-01-31T00:00:00Z"},
		{"sql.NullTime null", sql.NullTime{}, arrow.FixedWidthTypes.Timestamp_ns, true, nil},
		{"sql.Null[int32] valid", sql.Null[int32]{V: 4, Valid: true}, arrow.PrimitiveTypes.Int32, false, int32(4)},
		{"sql.Null[vendorID] null", sql.Null[vendorID]{}, arrow.PrimitiveTypes.Int64, true, nil},
		{"custom driver.Valuer", upperValuer("cmt"), arrow.BinaryTypes.String, false, "CMT"},
		{"pointer-receiver driver.Valuer", &pv, arrow.BinaryTypes.String, false, "pv-5"},
		{"Param over a pointer", NewParam(&n), arrow.PrimitiveTypes.Int64, false, int64(7)},
		{"typed Param over a null", NewTypedParam(sql.NullInt64{}, arrow.PrimitiveTypes.Int32), arrow.PrimitiveTypes.Int32, true, nil},
		{"typed Param over a nil unsupported pointer", NewTypedParam((*struct{ X int })(nil), arrow.BinaryTypes.String), arrow.BinaryTypes.String, true, nil},
		{"typed Param over an invalid sql.Null of an unsupported type", NewTypedParam(sql.Null[struct{ X int }]{}, arrow.PrimitiveTypes.Int64), arrow.PrimitiveTypes.Int64, true, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bindForTest(t, tt.value)
			if !arrow.TypeEqual(got.fields[0].Type, tt.wantType) {
				t.Fatalf("type = %v, want %v", got.fields[0].Type, tt.wantType)
			}
			if got.nulls[0] != tt.wantNull {
				t.Fatalf("null = %v, want %v", got.nulls[0], tt.wantNull)
			}
			if tt.wantNull {
				return
			}
			// GetOneForMarshal renders a timestamp as RFC 3339 and a []byte as is.
			if fmt.Sprint(got.values[0]) != fmt.Sprint(tt.want) {
				t.Fatalf("value = %v, want %v", got.values[0], tt.want)
			}
		})
	}
}

func TestBindParametersKeepsSupportedValuesUnchanged(t *testing.T) {
	// time.Duration is a named int64, and arrow.Date32 a named int32: both keep
	// the Arrow type inferArrowType gives them rather than decaying to an integer.
	got := bindForTest(t, 30*time.Minute, arrow.Date32(19753), int64(1), nil)
	want := []arrow.DataType{
		arrow.FixedWidthTypes.Duration_ns,
		arrow.PrimitiveTypes.Date32,
		arrow.PrimitiveTypes.Int64,
		arrow.Null,
	}
	for i, field := range got.fields {
		if !arrow.TypeEqual(field.Type, want[i]) {
			t.Errorf("parameter %d type = %v, want %v", i, field.Type, want[i])
		}
	}
}

func TestBindParametersReportsUnusableValues(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		wantErr string
	}{
		{"driver.Valuer error", failingValuer{}, "no value"},
		{"self-returning driver.Valuer", selfValuer{}, "levels of pointers or driver.Valuer results"},
		{"unsupported struct", struct{ X int }{1}, "unsupported parameter type"},
		{"pointer to unsupported", &struct{ X int }{1}, "unsupported parameter type"},
		{"nil pointer to unsupported", (*struct{ X int })(nil), "unsupported parameter type"},
		{"slice of a named byte", namedBytes{1, 2}, "unsupported parameter type"},
		{"map", map[string]int{"a": 1}, "unsupported parameter type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := (&SpiceClient{}).bindParameters(&ADBCClient{mem: memory.NewGoAllocator()}, &bindCapture{}, tt.value)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("bindParameters(%T) error = %v, want one containing %q", tt.value, err, tt.wantErr)
			}
		})
	}
}
