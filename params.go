package gospice

import (
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
