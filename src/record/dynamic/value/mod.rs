mod cast;
mod encoding;
mod util;
mod value_ref;
use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::datatypes::DataType;
pub use cast::*;
use thiserror::Error;
pub(crate) use util::*;
pub use value_ref::*;

use crate::record::{Key, TimeUnit};

#[derive(Debug, Error)]
pub enum ValueError {
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
    #[error("Invalid conversion: {0}")]
    InvalidConversion(String),
    #[error("Null value not allowed")]
    NullNotAllowed,
    #[error("Invalid data type: can not convert data from {0}")]
    InvalidDataType(String),
}

/// A value in the [`DynRecord`].
#[derive(Debug, Clone)]
pub enum Value {
    /// Null is less than any non-Null value
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    /// A binary array of fixed size.
    /// The first parameter specifies the value and the second parameter specifies the number of
    /// bytes of value
    FixedSizeBinary(Vec<u8>, u32),
    Date32(i32),
    Date64(i64),
    Time32(i32, TimeUnit),
    Time64(i64, TimeUnit),
    Timestamp(i64, TimeUnit),
}

impl Value {
    /// Get the arrow data type of the value.
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Boolean(_) => DataType::Boolean,
            Value::Int8(_) => DataType::Int8,
            Value::Int16(_) => DataType::Int16,
            Value::Int32(_) => DataType::Int32,
            Value::Int64(_) => DataType::Int64,
            Value::UInt8(_) => DataType::UInt8,
            Value::UInt16(_) => DataType::UInt16,
            Value::UInt32(_) => DataType::UInt32,
            Value::UInt64(_) => DataType::UInt64,
            Value::Float32(_) => DataType::Float32,
            Value::Float64(_) => DataType::Float64,
            Value::String(_) => DataType::Utf8,
            Value::Binary(_) => DataType::Binary,
            Value::FixedSizeBinary(_, byte_width) => DataType::FixedSizeBinary(*byte_width as i32),
            Value::Date32(_) => DataType::Date32,
            Value::Date64(_) => DataType::Date64,
            Value::Timestamp(_, unit) => {
                let arrow_unit = match unit {
                    TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
                    TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
                    TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
                    TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
                };
                DataType::Timestamp(arrow_unit, None)
            }
            Value::Time32(_, unit) => {
                let arrow_unit = match unit {
                    TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
                    TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
                    _ => unreachable!("Time32 only supports second and millisecond"),
                };
                DataType::Time32(arrow_unit)
            }
            Value::Time64(_, unit) => {
                let arrow_unit = match unit {
                    TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
                    TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
                    _ => unreachable!("Time64 only supports microsecond and nanosecond"),
                };
                DataType::Time64(arrow_unit)
            }
        }
    }

    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl Key for Value {
    type Ref<'r> = ValueRef<'r>;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        match self {
            Value::Null => ValueRef::Null,
            Value::Boolean(v) => ValueRef::Boolean(*v),
            Value::Int8(v) => ValueRef::Int8(*v),
            Value::Int16(v) => ValueRef::Int16(*v),
            Value::Int32(v) => ValueRef::Int32(*v),
            Value::Int64(v) => ValueRef::Int64(*v),
            Value::UInt8(v) => ValueRef::UInt8(*v),
            Value::UInt16(v) => ValueRef::UInt16(*v),
            Value::UInt32(v) => ValueRef::UInt32(*v),
            Value::UInt64(v) => ValueRef::UInt64(*v),
            Value::Float32(v) => ValueRef::Float32(*v),
            Value::Float64(v) => ValueRef::Float64(*v),
            Value::String(v) => ValueRef::String(v.as_str()),
            Value::Binary(v) => ValueRef::Binary(v.as_slice()),
            Value::FixedSizeBinary(v, byte_width) => {
                ValueRef::FixedSizeBinary(v.as_slice(), *byte_width)
            }
            Value::Date32(v) => ValueRef::Date32(*v),
            Value::Date64(v) => ValueRef::Date64(*v),
            Value::Timestamp(v, time_unit) => ValueRef::Timestamp(*v, *time_unit),
            Value::Time32(v, time_unit) => ValueRef::Time32(*v, *time_unit),
            Value::Time64(v, time_unit) => ValueRef::Time64(*v, *time_unit),
        }
    }

    fn to_arrow_datum(&self) -> Arc<dyn arrow::array::Datum> {
        match self {
            Value::Null => panic!("Null value cannot be converted to arrow datum"),
            Value::Boolean(v) => Arc::new(arrow::array::BooleanArray::new_scalar(*v)),
            Value::Int8(v) => Arc::new(arrow::array::Int8Array::new_scalar(*v)),
            Value::Int16(v) => Arc::new(arrow::array::Int16Array::new_scalar(*v)),
            Value::Int32(v) => Arc::new(arrow::array::Int32Array::new_scalar(*v)),
            Value::Int64(v) => Arc::new(arrow::array::Int64Array::new_scalar(*v)),
            Value::UInt8(v) => Arc::new(arrow::array::UInt8Array::new_scalar(*v)),
            Value::UInt16(v) => Arc::new(arrow::array::UInt16Array::new_scalar(*v)),
            Value::UInt32(v) => Arc::new(arrow::array::UInt32Array::new_scalar(*v)),
            Value::UInt64(v) => Arc::new(arrow::array::UInt64Array::new_scalar(*v)),
            Value::Float32(v) => Arc::new(arrow::array::Float32Array::new_scalar(*v)),
            Value::Float64(v) => Arc::new(arrow::array::Float64Array::new_scalar(*v)),
            Value::String(v) => Arc::new(arrow::array::StringArray::new_scalar(v.as_str())),
            Value::Binary(v) => Arc::new(arrow::array::BinaryArray::new_scalar(v.as_slice())),
            Value::FixedSizeBinary(v, _) => {
                Arc::new(arrow::array::FixedSizeBinaryArray::new_scalar(v.as_slice()))
            }
            Value::Date32(v) => Arc::new(arrow::array::Date32Array::new_scalar(*v)),
            Value::Date64(v) => Arc::new(arrow::array::Date64Array::new_scalar(*v)),
            Value::Timestamp(v, time_unit) => match time_unit {
                TimeUnit::Second => Arc::new(arrow::array::TimestampSecondArray::new_scalar(*v)),
                TimeUnit::Millisecond => {
                    Arc::new(arrow::array::TimestampMillisecondArray::new_scalar(*v))
                }
                TimeUnit::Microsecond => {
                    Arc::new(arrow::array::TimestampMicrosecondArray::new_scalar(*v))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(arrow::array::TimestampNanosecondArray::new_scalar(*v))
                }
            },
            Value::Time32(v, time_unit) => match time_unit {
                TimeUnit::Second => Arc::new(arrow::array::Time32SecondArray::new_scalar(*v)),
                TimeUnit::Millisecond => {
                    Arc::new(arrow::array::Time32MillisecondArray::new_scalar(*v))
                }
                _ => unreachable!("Time32 only supports second and millisecond"),
            },
            Value::Time64(v, time_unit) => match time_unit {
                TimeUnit::Microsecond => {
                    Arc::new(arrow::array::Time64MicrosecondArray::new_scalar(*v))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(arrow::array::Time64NanosecondArray::new_scalar(*v))
                }
                _ => unreachable!("Time64 only supports microsecond and nanosecond"),
            },
        }
    }
}

impl Eq for Value {}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Null, _) => false,
            (_, Value::Null) => false,
            (Value::Boolean(a), Value::Boolean(b)) => a.eq(b),
            (Value::Int8(a), Value::Int8(b)) => a.eq(b),
            (Value::Int16(a), Value::Int16(b)) => a.eq(b),
            (Value::Int32(a), Value::Int32(b)) => a.eq(b),
            (Value::Int64(a), Value::Int64(b)) => a.eq(b),
            (Value::UInt8(a), Value::UInt8(b)) => a.eq(b),
            (Value::UInt16(a), Value::UInt16(b)) => a.eq(b),
            (Value::UInt32(a), Value::UInt32(b)) => a.eq(b),
            (Value::UInt64(a), Value::UInt64(b)) => a.eq(b),
            (Value::Float32(a), Value::Float32(b)) => a.to_bits() == b.to_bits(),
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a.eq(b),
            (Value::Binary(a), Value::Binary(b)) => a.eq(b),
            (Value::FixedSizeBinary(a, _), Value::FixedSizeBinary(b, _)) => a.eq(b),
            (Value::Date32(a), Value::Date32(b)) => a.eq(b),
            (Value::Date64(a), Value::Date64(b)) => a.eq(b),
            (Value::Timestamp(a, unit1), Value::Timestamp(b, unit2)) => {
                if unit1 == unit2 {
                    return a.eq(b);
                }
                let (s_sec, s_nsec) = split_second_ns(*a, *unit1);
                let (o_sec, o_nsec) = split_second_ns(*b, *unit2);
                s_sec == o_sec && s_nsec == o_nsec
            }
            (Value::Time32(a, unit1), Value::Time32(b, unit2)) => {
                if unit1 == unit2 {
                    return a.eq(b);
                }
                let (s_sec, s_nsec) = split_second_ns(*a as i64, *unit1);
                let (o_sec, o_nsec) = split_second_ns(*b as i64, *unit2);
                s_sec == o_sec && s_nsec == o_nsec
            }
            (Value::Time64(a, unit1), Value::Time64(b, unit2)) => {
                if unit1 == unit2 {
                    return a.eq(b);
                }
                let (s_sec, s_nsec) = split_second_ns(*a, *unit1);
                let (o_sec, o_nsec) = split_second_ns(*b, *unit2);
                s_sec == o_sec && s_nsec == o_nsec
            }
            _ => {
                panic!("cannot compare different types: {self:?} and {other:?}")
            }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Int8(a), Value::Int8(b)) => a.cmp(b),
            (Value::Int16(a), Value::Int16(b)) => a.cmp(b),
            (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::UInt8(a), Value::UInt8(b)) => a.cmp(b),
            (Value::UInt16(a), Value::UInt16(b)) => a.cmp(b),
            (Value::UInt32(a), Value::UInt32(b)) => a.cmp(b),
            (Value::UInt64(a), Value::UInt64(b)) => a.cmp(b),
            (Value::Float32(a), Value::Float32(b)) => a.total_cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.total_cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Binary(a), Value::Binary(b)) => a.cmp(b),
            (Value::FixedSizeBinary(a, _), Value::FixedSizeBinary(b, _)) => a.cmp(b),
            (Value::Date32(a), Value::Date32(b)) => a.cmp(b),
            (Value::Date64(a), Value::Date64(b)) => a.cmp(b),
            (Value::Timestamp(a, unit1), Value::Timestamp(b, unit2)) => {
                if unit1 == unit2 {
                    return a.cmp(b);
                }
                let (s_sec, s_nsec) = split_second_ns(*a, *unit1);
                let (o_sec, o_nsec) = split_second_ns(*b, *unit2);
                match s_sec.cmp(&o_sec) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Greater => Ordering::Greater,
                    Ordering::Equal => s_nsec.cmp(&o_nsec),
                }
            }
            _ => {
                panic!("cannot compare different types: {self:?} and {other:?}")
            }
        }
    }
}

impl Hash for Value {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        match self {
            Value::Null => 0_u8.hash(state),
            Value::Boolean(v) => v.hash(state),
            Value::Int8(v) => v.hash(state),
            Value::Int16(v) => v.hash(state),
            Value::Int32(v) => v.hash(state),
            Value::Int64(v) => v.hash(state),
            Value::UInt8(v) => v.hash(state),
            Value::UInt16(v) => v.hash(state),
            Value::UInt32(v) => v.hash(state),
            Value::UInt64(v) => v.hash(state),
            Value::Float32(v) => v.to_bits().hash(state),
            Value::Float64(v) => v.to_bits().hash(state),
            Value::String(v) => v.hash(state),
            Value::Binary(vec) => vec.hash(state),
            Value::FixedSizeBinary(v, byte_width) => {
                byte_width.hash(state);
                v.hash(state)
            }
            Value::Date32(v) => v.hash(state),
            Value::Date64(v) => v.hash(state),
            Value::Timestamp(v, time_unit) => {
                v.hash(state);
                time_unit.hash(state);
            }
            Value::Time32(v, time_unit) => {
                v.hash(state);
                time_unit.hash(state);
            }
            Value::Time64(v, time_unit) => {
                v.hash(state);
                time_unit.hash(state);
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::Int8(v) => write!(f, "{v}"),
            Value::Int16(v) => write!(f, "{v}"),
            Value::Int32(v) => write!(f, "{v}"),
            Value::Int64(v) => write!(f, "{v}"),
            Value::UInt8(v) => write!(f, "{v}"),
            Value::UInt16(v) => write!(f, "{v}"),
            Value::UInt32(v) => write!(f, "{v}"),
            Value::UInt64(v) => write!(f, "{v}"),
            Value::Float32(v) => write!(f, "{v}"),
            Value::Float64(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{v}"),
            Value::Binary(v) => write!(f, "{v:?}"),
            Value::FixedSizeBinary(v, byte_width) => write!(f, "{v:?}, {byte_width}"),
            Value::Date32(v) => write!(f, "Date32({v})"),
            Value::Date64(v) => write!(f, "Date64({v})"),
            Value::Timestamp(v, unit) => write!(f, "Timestamp({v}, {unit:?})"),
            Value::Time32(v, unit) => write!(f, "Time32({v}, {unit:?})"),
            Value::Time64(v, unit) => write!(f, "Time64({v}, {unit:?})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::record::{AsValue, TimeUnit, Value};

    #[test]
    fn test_value_basic_types() {
        assert_eq!(Value::Null.data_type(), DataType::Null);
        assert_eq!(Value::Boolean(true).data_type(), DataType::Boolean);
        assert_eq!(Value::Int32(42).data_type(), DataType::Int32);
        assert_eq!(Value::Int64(42).data_type(), DataType::Int64);
        assert_eq!(
            Value::Float32(std::f32::consts::PI).data_type(),
            DataType::Float32
        );
        assert_eq!(
            Value::Float64(std::f64::consts::PI).data_type(),
            DataType::Float64
        );
        assert_eq!(Value::String("hello".into()).data_type(), DataType::Utf8);
        assert_eq!(Value::Binary(vec![1, 2, 3]).data_type(), DataType::Binary);
    }

    #[test]
    fn test_value_null_checks() {
        assert!(Value::Null.is_null());
        assert!(!Value::Boolean(false).is_null());
        assert!(!Value::Int32(0).is_null());
    }

    #[test]
    fn test_value_conversions() {
        assert!(Value::Boolean(true).as_bool_opt().unwrap());
        assert_eq!(*Value::UInt8(42).as_u8_opt().unwrap(), 42u8);
        assert_eq!(*Value::Int32(42).as_i32_opt().unwrap(), 42i32);
        assert!(
            (Value::Float64(std::f64::consts::PI).as_f64_opt().unwrap() - std::f64::consts::PI)
                .abs()
                < 0.0001
        );
        assert_eq!(
            Value::String("hello".into()).as_string_opt().unwrap(),
            "hello"
        );
        assert_eq!(
            Value::Binary(vec![1, 2, 3]).as_bytes_opt().unwrap(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn test_binary_value_conversions() {
        assert_eq!(Value::Binary(vec![1, 2, 3]).as_bytes(), &[1, 2, 3]);
        assert_eq!(
            Value::FixedSizeBinary(vec![1, 2, 3], 3).as_bytes(),
            &[1, 2, 3]
        );
        assert_eq!(
            Value::FixedSizeBinary(vec![1, 2, 3, 4, 5], 5).as_bytes(),
            &[1, 2, 3, 4, 5]
        );
        assert_eq!(Value::UInt8(1).as_bytes_opt(), None);
    }

    #[test]
    fn test_value_conversion_fail() {
        assert!(Value::Int32(42).as_bool_opt().is_none());
        assert!(Value::String("hello".into()).as_i64_opt().is_none());
        assert!(Value::Boolean(true).as_string_opt().is_none());
    }

    #[test]
    fn test_float_value_cmp() {
        // test zero
        {
            let zero = Value::Float32(0.0_f32);
            let neg_zero = Value::Float32(-0.0_f32);
            assert!(zero > neg_zero);
        }
        // test NAN and INF
        {
            let nan1 = Value::Float32(f32::NAN);
            let nan2 = Value::Float32(f32::NAN);
            let neg_nan = Value::Float32(-f32::NAN);
            let inf = Value::Float32(f32::INFINITY);
            let neg_inf = Value::Float32(f32::NEG_INFINITY);

            // This is not consistent with the IEEE, but it's consistent with the Arrow

            assert_eq!(nan1, nan2);
            assert!(nan1 > neg_nan);
            // negative NAN should be less than negative infinity
            assert!(neg_nan < neg_inf);
            // positive NAN should be greater than positive infinity
            assert!(nan1 > inf);
        }
        {
            let f1 = Value::Float32(1.0_f32);
            let f2 = Value::Float32(2.1_f32);
            let f3 = Value::Float32(2.1_f32);
            assert!(f1 < f2);
            assert!(f2 == f3);
        }
    }

    #[test]
    fn test_timestamp_value_cmp() {
        let t1 = Value::Timestamp(1716, TimeUnit::Second);
        let t2 = Value::Timestamp(1716000, TimeUnit::Millisecond);
        let t3 = Value::Timestamp(1716000001, TimeUnit::Microsecond);
        let t4 = Value::Timestamp(1715999999999, TimeUnit::Nanosecond);
        assert!(t1 == t2);
        assert!(t1 < t3);
        assert!(t1 > t4);
    }
}
