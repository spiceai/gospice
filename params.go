package gospice

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// Param represents a query parameter with an optional explicit Arrow type.
// If Type is nil, the type will be inferred from the Value.
type Param struct {
	Value any
	Type  arrow.DataType
}

// NewParam creates a new parameter with inferred type
func NewParam(value any) Param {
	return Param{Value: value, Type: nil}
}

// NewTypedParam creates a new parameter with explicit type
func NewTypedParam(value any, dataType arrow.DataType) Param {
	return Param{Value: value, Type: dataType}
}

// Common type constructors for convenience

// Int8Param creates an int8 parameter
func Int8Param(value int8) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Int8)
}

// Int16Param creates an int16 parameter
func Int16Param(value int16) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Int16)
}

// Int32Param creates an int32 parameter
func Int32Param(value int32) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Int32)
}

// Int64Param creates an int64 parameter
func Int64Param(value int64) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Int64)
}

// Uint8Param creates a uint8 parameter
func Uint8Param(value uint8) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Uint8)
}

// Uint16Param creates a uint16 parameter
func Uint16Param(value uint16) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Uint16)
}

// Uint32Param creates a uint32 parameter
func Uint32Param(value uint32) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Uint32)
}

// Uint64Param creates a uint64 parameter
func Uint64Param(value uint64) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Uint64)
}

// Float16Param creates a float16 parameter
func Float16Param(value uint16) Param {
	return NewTypedParam(value, arrow.FixedWidthTypes.Float16)
}

// Float32Param creates a float32 parameter
func Float32Param(value float32) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Float32)
}

// Float64Param creates a float64 parameter
func Float64Param(value float64) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Float64)
}

// StringParam creates a string parameter
func StringParam(value string) Param {
	return NewTypedParam(value, arrow.BinaryTypes.String)
}

// LargeStringParam creates a large string parameter
func LargeStringParam(value string) Param {
	return NewTypedParam(value, arrow.BinaryTypes.LargeString)
}

// BinaryParam creates a binary parameter
func BinaryParam(value []byte) Param {
	return NewTypedParam(value, arrow.BinaryTypes.Binary)
}

// LargeBinaryParam creates a large binary parameter
func LargeBinaryParam(value []byte) Param {
	return NewTypedParam(value, arrow.BinaryTypes.LargeBinary)
}

// BoolParam creates a boolean parameter
func BoolParam(value bool) Param {
	return NewTypedParam(value, arrow.FixedWidthTypes.Boolean)
}

// Date32Param creates a Date32 parameter
func Date32Param(value arrow.Date32) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Date32)
}

// Date64Param creates a Date64 parameter
func Date64Param(value arrow.Date64) Param {
	return NewTypedParam(value, arrow.PrimitiveTypes.Date64)
}

// Time32Param creates a Time32 parameter with specified unit
func Time32Param(value arrow.Time32, unit arrow.TimeUnit) Param {
	return NewTypedParam(value, &arrow.Time32Type{Unit: unit})
}

// Time64Param creates a Time64 parameter with specified unit
func Time64Param(value arrow.Time64, unit arrow.TimeUnit) Param {
	return NewTypedParam(value, &arrow.Time64Type{Unit: unit})
}

// TimestampParam creates a Timestamp parameter with specified unit and timezone
func TimestampParam(value arrow.Timestamp, unit arrow.TimeUnit, timezone string) Param {
	return NewTypedParam(value, &arrow.TimestampType{Unit: unit, TimeZone: timezone})
}

// DurationParam creates a Duration parameter with specified unit
func DurationParam(value arrow.Duration, unit arrow.TimeUnit) Param {
	return NewTypedParam(value, &arrow.DurationType{Unit: unit})
}

// MonthIntervalParam creates a MonthInterval parameter
func MonthIntervalParam(value arrow.MonthInterval) Param {
	return NewTypedParam(value, arrow.FixedWidthTypes.MonthInterval)
}

// DayTimeIntervalParam creates a DayTimeInterval parameter
func DayTimeIntervalParam(value arrow.DayTimeInterval) Param {
	return NewTypedParam(value, arrow.FixedWidthTypes.DayTimeInterval)
}

// MonthDayNanoIntervalParam creates a MonthDayNanoInterval parameter
func MonthDayNanoIntervalParam(value arrow.MonthDayNanoInterval) Param {
	return NewTypedParam(value, arrow.FixedWidthTypes.MonthDayNanoInterval)
}

// Decimal128Param creates a Decimal128 parameter with specified precision and scale
func Decimal128Param(value [16]byte, precision, scale int32) Param {
	return NewTypedParam(value, &arrow.Decimal128Type{Precision: precision, Scale: scale})
}

// Decimal256Param creates a Decimal256 parameter with specified precision and scale
func Decimal256Param(value [32]byte, precision, scale int32) Param {
	return NewTypedParam(value, &arrow.Decimal256Type{Precision: precision, Scale: scale})
}

// FixedSizeBinaryParam creates a FixedSizeBinary parameter with specified byte width
func FixedSizeBinaryParam(value []byte, byteWidth int) Param {
	return NewTypedParam(value, &arrow.FixedSizeBinaryType{ByteWidth: byteWidth})
}

// NullParam creates a null parameter
func NullParam() Param {
	return NewTypedParam(nil, arrow.Null)
}

// maxParamIndirection bounds how many pointers and driver.Valuer results
// normalizeParamValue follows, so a Valuer that returns itself cannot loop.
const maxParamIndirection = 16

// normalizeParamValue unwraps the forms Go code commonly holds a bind value in
// into one inferArrowType and appendValueToBuilder accept: a pointer, a
// database/sql Null type or any other driver.Valuer, or a named type over a
// basic kind (type VendorID int64). A value inferArrowType already accepts is
// returned unchanged.
//
// A NULL comes back as a nil value with the Arrow type the value would have had
// as nullType, so the parameter stays typed: a nil *int64 binds as an Int64
// NULL, not an untyped one. nullType is nil when the NULL carries no type, as
// with a driver.Valuer that returns nil.
func normalizeParamValue(val any) (value any, nullType arrow.DataType, err error) {
	return normalizeParamValueDepth(val, 0)
}

func normalizeParamValueDepth(val any, depth int) (any, arrow.DataType, error) {
	if depth > maxParamIndirection {
		return nil, nil, fmt.Errorf("parameter of type %T: more than %d levels of pointers or driver.Valuer results", val, maxParamIndirection)
	}
	if val == nil {
		return nil, nil, nil
	}
	if _, err := inferArrowType(val); err == nil {
		return val, nil, nil
	}

	switch v := val.(type) {
	case sql.NullString:
		return nullable(v.Valid, v.String)
	case sql.NullInt64:
		return nullable(v.Valid, v.Int64)
	case sql.NullInt32:
		return nullable(v.Valid, v.Int32)
	case sql.NullInt16:
		return nullable(v.Valid, v.Int16)
	case sql.NullByte:
		return nullable(v.Valid, v.Byte)
	case sql.NullFloat64:
		return nullable(v.Valid, v.Float64)
	case sql.NullBool:
		return nullable(v.Valid, v.Bool)
	case sql.NullTime:
		return nullable(v.Valid, v.Time)
	}

	rv := reflect.ValueOf(val)
	rt := rv.Type()

	// sql.Null[T] is generic, so it cannot appear in a type switch.
	if rt.Kind() == reflect.Struct && rt.PkgPath() == "database/sql" && strings.HasPrefix(rt.Name(), "Null[") {
		if !rv.FieldByName("Valid").Bool() {
			field, _ := rt.FieldByName("V")
			return typedNull(reflect.Zero(field.Type).Interface(), depth)
		}
		return normalizeParamValueDepth(rv.FieldByName("V").Interface(), depth+1)
	}

	if rt.Kind() == reflect.Pointer {
		pointerValuer := rt.Implements(valuerType) && !rt.Elem().Implements(valuerType)
		if rv.IsNil() {
			if pointerValuer {
				// Value() cannot be called on a nil receiver, so the type it
				// would bind as is unknown; only an explicit type can bind it.
				return nil, nil, &untypedNullError{fmt.Errorf("parameter of type %T: nil pointer to a driver.Valuer with a pointer receiver has no inferable type (use NewTypedParam for explicit type control)", val)}
			}
			return typedNull(reflect.Zero(rt.Elem()).Interface(), depth)
		}
		// A Valuer with a pointer receiver is lost once the pointer is
		// dereferenced, so call it here. One with a value receiver, such as
		// sql.NullInt64, is unwrapped below where its NULL can stay typed.
		if pointerValuer {
			return valueOf(val.(driver.Valuer), depth)
		}
		return normalizeParamValueDepth(rv.Elem().Interface(), depth+1)
	}

	if valuer, ok := val.(driver.Valuer); ok {
		return valueOf(valuer, depth)
	}

	if basic, ok := basicKindTypes[rt.Kind()]; ok && rt.ConvertibleTo(basic) {
		return rv.Convert(basic).Interface(), nil, nil
	}
	if bytesType := reflect.TypeFor[[]byte](); rt.Kind() == reflect.Slice && rt.ConvertibleTo(bytesType) {
		return rv.Convert(bytesType).Interface(), nil, nil
	}

	// Leave the value as it is, so inferArrowType reports it as unsupported.
	return val, nil, nil
}

var valuerType = reflect.TypeFor[driver.Valuer]()

// valueOf normalizes the value a driver.Valuer returns.
func valueOf(valuer driver.Valuer, depth int) (any, arrow.DataType, error) {
	driverValue, err := valuer.Value()
	if err != nil {
		return nil, nil, fmt.Errorf("parameter of type %T: driver.Valuer: %w", valuer, err)
	}
	return normalizeParamValueDepth(driverValue, depth+1)
}

// untypedNullError reports a NULL whose Arrow type could not be inferred from
// the type it was held in. The value is still a NULL, so a parameter with an
// explicit type can bind it.
type untypedNullError struct{ err error }

func (e *untypedNullError) Error() string { return e.err.Error() }
func (e *untypedNullError) Unwrap() error { return e.err }

// nullable returns value when valid and otherwise a NULL typed like value.
func nullable(valid bool, value any) (any, arrow.DataType, error) {
	if valid {
		return value, nil, nil
	}
	return typedNull(value, 0)
}

// typedNull returns a NULL with the Arrow type zero would bind as.
func typedNull(zero any, depth int) (any, arrow.DataType, error) {
	value, nullType, err := normalizeParamValueDepth(zero, depth+1)
	if err != nil {
		return nil, nil, &untypedNullError{err}
	}
	if value == nil {
		return nil, nullType, nil
	}
	dataType, err := inferArrowType(value)
	if err != nil {
		return nil, nil, &untypedNullError{err}
	}
	return nil, dataType, nil
}

// basicKindTypes maps each basic kind to the unnamed type inferArrowType knows.
var basicKindTypes = map[reflect.Kind]reflect.Type{
	reflect.Bool:    reflect.TypeFor[bool](),
	reflect.Int:     reflect.TypeFor[int](),
	reflect.Int8:    reflect.TypeFor[int8](),
	reflect.Int16:   reflect.TypeFor[int16](),
	reflect.Int32:   reflect.TypeFor[int32](),
	reflect.Int64:   reflect.TypeFor[int64](),
	reflect.Uint:    reflect.TypeFor[uint](),
	reflect.Uint8:   reflect.TypeFor[uint8](),
	reflect.Uint16:  reflect.TypeFor[uint16](),
	reflect.Uint32:  reflect.TypeFor[uint32](),
	reflect.Uint64:  reflect.TypeFor[uint64](),
	reflect.Float32: reflect.TypeFor[float32](),
	reflect.Float64: reflect.TypeFor[float64](),
	reflect.String:  reflect.TypeFor[string](),
}
