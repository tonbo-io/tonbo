use std::cmp::Ordering;

use arrow::{
    array::{
        ArrayRef, AsArray, Date32Array, Date64Array, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::{
        DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        TimeUnit as ArrowTimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};

use crate::record::{split_second_ns, KeyRef, TimeUnit, Value, ValueError};

/// A reference type for Value that avoids cloning
#[derive(Debug, Clone, Copy)]
pub enum ValueRef<'a> {
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
    String(&'a str),
    Binary(&'a [u8]),
    Date32(i32),
    Date64(i64),
    Timestamp(i64, TimeUnit),
    Time32(i32, TimeUnit),
    Time64(i64, TimeUnit),
}

/// A reference type for [`Value`] that avoids cloning
impl<'a> ValueRef<'a> {
    /// Get value from arrow array ref at the given index
    pub fn from_array_ref(array: &'a ArrayRef, index: usize) -> Result<Self, ValueError> {
        if index >= array.len() {
            return Err(ValueError::InvalidConversion(
                "Index out of bounds".to_string(),
            ));
        }

        if array.is_null(index) {
            return Ok(ValueRef::Null);
        }

        match array.data_type() {
            DataType::Null => Ok(ValueRef::Null),
            DataType::Boolean => {
                let arr = array
                    .as_boolean_opt()
                    .ok_or_else(|| ValueError::InvalidConversion("Boolean cast failed".into()))?;
                Ok(ValueRef::Boolean(arr.value(index)))
            }
            DataType::Int8 => {
                let arr = array
                    .as_primitive_opt::<Int8Type>()
                    .ok_or_else(|| ValueError::InvalidConversion("Int8 cast failed".into()))?;
                Ok(ValueRef::Int8(arr.value(index)))
            }
            DataType::Int16 => {
                let arr = array
                    .as_primitive_opt::<Int16Type>()
                    .ok_or_else(|| ValueError::InvalidConversion("Int16 cast failed".into()))?;
                Ok(ValueRef::Int16(arr.value(index)))
            }
            DataType::Int32 => {
                let arr = array
                    .as_primitive_opt::<Int32Type>()
                    .ok_or_else(|| ValueError::InvalidConversion("Int32 cast failed".into()))?;
                Ok(ValueRef::Int32(arr.value(index)))
            }
            DataType::Int64 => {
                let arr = array
                    .as_primitive_opt::<Int64Type>()
                    .ok_or_else(|| ValueError::InvalidConversion("Int64 cast failed".into()))?;
                Ok(ValueRef::Int64(arr.value(index)))
            }
            DataType::UInt8 => {
                let arr = array
                    .as_primitive_opt::<UInt8Type>()
                    .ok_or_else(|| ValueError::InvalidConversion("UInt8 cast failed".into()))?;
                Ok(ValueRef::UInt8(arr.value(index)))
            }
            DataType::UInt16 => {
                let arr = array
                    .as_primitive_opt::<UInt16Type>()
                    .ok_or_else(|| ValueError::InvalidConversion("UInt16 cast failed".into()))?;
                Ok(ValueRef::UInt16(arr.value(index)))
            }
            DataType::UInt32 => {
                let arr = array
                    .as_primitive_opt::<UInt32Type>()
                    .ok_or_else(|| ValueError::InvalidConversion("UInt32 cast failed".into()))?;
                Ok(ValueRef::UInt32(arr.value(index)))
            }
            DataType::UInt64 => {
                let arr = array
                    .as_primitive_opt::<UInt64Type>()
                    .ok_or_else(|| ValueError::InvalidConversion("UInt64 cast failed".into()))?;
                Ok(ValueRef::UInt64(arr.value(index)))
            }
            DataType::Float32 => {
                let arr = array
                    .as_primitive_opt::<Float32Type>()
                    .ok_or_else(|| ValueError::InvalidConversion("Float32 cast failed".into()))?;
                Ok(ValueRef::Float32(arr.value(index)))
            }
            DataType::Float64 => {
                let arr = array
                    .as_primitive_opt::<Float64Type>()
                    .ok_or_else(|| ValueError::InvalidConversion("Float64 cast failed".into()))?;
                Ok(ValueRef::Float64(arr.value(index)))
            }
            DataType::Utf8 => {
                let arr = array
                    .as_string_opt::<i32>()
                    .ok_or_else(|| ValueError::InvalidConversion("Utf8 cast failed".into()))?;
                Ok(ValueRef::String(arr.value(index)))
            }
            DataType::Binary => {
                let arr = array
                    .as_binary_opt::<i32>()
                    .ok_or_else(|| ValueError::InvalidConversion("Binary cast failed".into()))?;
                Ok(ValueRef::Binary(arr.value(index)))
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("Date32 cast failed".into()))?;
                Ok(ValueRef::Date32(arr.value(index)))
            }
            DataType::Date64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date64Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("Date64 cast failed".into()))?;
                Ok(ValueRef::Date64(arr.value(index)))
            }
            DataType::Timestamp(unit, _) => match unit {
                ArrowTimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| {
                            ValueError::InvalidConversion("TimestampSecond cast failed".into())
                        })?;
                    Ok(ValueRef::Timestamp(arr.value(index), TimeUnit::Second))
                }
                ArrowTimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            ValueError::InvalidConversion("TimestampMillisecond cast failed".into())
                        })?;
                    Ok(ValueRef::Timestamp(arr.value(index), TimeUnit::Millisecond))
                }
                ArrowTimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            ValueError::InvalidConversion("TimestampMicrosecond cast failed".into())
                        })?;
                    Ok(ValueRef::Timestamp(arr.value(index), TimeUnit::Microsecond))
                }
                ArrowTimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            ValueError::InvalidConversion("TimestampNanosecond cast failed".into())
                        })?;
                    Ok(ValueRef::Timestamp(arr.value(index), TimeUnit::Nanosecond))
                }
            },
            _ => Err(ValueError::InvalidConversion(format!(
                "Unsupported data type: {:?}",
                array.data_type()
            ))),
        }
    }

    /// Get the arrow data type of the value.
    pub fn data_type(&self) -> DataType {
        match self {
            ValueRef::Null => DataType::Null,
            ValueRef::Boolean(_) => DataType::Boolean,
            ValueRef::Int8(_) => DataType::Int8,
            ValueRef::Int16(_) => DataType::Int16,
            ValueRef::Int32(_) => DataType::Int32,
            ValueRef::Int64(_) => DataType::Int64,
            ValueRef::UInt8(_) => DataType::UInt8,
            ValueRef::UInt16(_) => DataType::UInt16,
            ValueRef::UInt32(_) => DataType::UInt32,
            ValueRef::UInt64(_) => DataType::UInt64,
            ValueRef::Float32(_) => DataType::Float32,
            ValueRef::Float64(_) => DataType::Float64,
            ValueRef::String(_) => DataType::Utf8,
            ValueRef::Binary(_) => DataType::Binary,
            ValueRef::Date32(_) => DataType::Date32,
            ValueRef::Date64(_) => DataType::Date64,
            ValueRef::Timestamp(_, unit) => {
                let arrow_unit = match unit {
                    TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
                    TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
                    TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
                    TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
                };
                DataType::Timestamp(arrow_unit, None)
            }
            ValueRef::Time32(_, unit) => {
                let arrow_unit = match unit {
                    TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
                    TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
                    _ => unreachable!("Time32 only supports second and millisecond"),
                };
                DataType::Time32(arrow_unit)
            }
            ValueRef::Time64(_, unit) => {
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
        matches!(self, ValueRef::Null)
    }
}

impl ValueRef<'_> {
    /// Convert to an owned Value
    pub fn to_owned(&self) -> Value {
        match *self {
            ValueRef::Null => Value::Null,
            ValueRef::Boolean(v) => Value::Boolean(v),
            ValueRef::Int8(v) => Value::Int8(v),
            ValueRef::Int16(v) => Value::Int16(v),
            ValueRef::Int32(v) => Value::Int32(v),
            ValueRef::Int64(v) => Value::Int64(v),
            ValueRef::UInt8(v) => Value::UInt8(v),
            ValueRef::UInt16(v) => Value::UInt16(v),
            ValueRef::UInt32(v) => Value::UInt32(v),
            ValueRef::UInt64(v) => Value::UInt64(v),
            ValueRef::Float32(v) => Value::Float32(v),
            ValueRef::Float64(v) => Value::Float64(v),
            ValueRef::String(s) => Value::String(s.to_string()),
            ValueRef::Binary(b) => Value::Binary(b.to_vec()),
            ValueRef::Date32(v) => Value::Date32(v),
            ValueRef::Date64(v) => Value::Date64(v),
            ValueRef::Timestamp(v, unit) => Value::Timestamp(v, unit),
            ValueRef::Time32(v, unit) => Value::Time32(v, unit),
            ValueRef::Time64(v, unit) => Value::Time64(v, unit),
        }
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
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
            Value::String(s) => ValueRef::String(s.as_str()),
            Value::Binary(b) => ValueRef::Binary(b.as_slice()),
            Value::Date32(v) => ValueRef::Date32(*v),
            Value::Date64(v) => ValueRef::Date64(*v),
            Value::Timestamp(v, unit) => ValueRef::Timestamp(*v, *unit),
            Value::Time32(v, unit) => ValueRef::Time32(*v, *unit),
            Value::Time64(v, unit) => ValueRef::Time64(*v, *unit),
        }
    }
}

impl Eq for ValueRef<'_> {}

impl PartialEq for ValueRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ValueRef::Null, ValueRef::Null) => true,
            (ValueRef::Null, _) => false,
            (_, ValueRef::Null) => false,
            (ValueRef::Boolean(a), ValueRef::Boolean(b)) => a.eq(b),
            (ValueRef::Int8(a), ValueRef::Int8(b)) => a.eq(b),
            (ValueRef::Int16(a), ValueRef::Int16(b)) => a.eq(b),
            (ValueRef::Int32(a), ValueRef::Int32(b)) => a.eq(b),
            (ValueRef::Int64(a), ValueRef::Int64(b)) => a.eq(b),
            (ValueRef::UInt8(a), ValueRef::UInt8(b)) => a.eq(b),
            (ValueRef::UInt16(a), ValueRef::UInt16(b)) => a.eq(b),
            (ValueRef::UInt32(a), ValueRef::UInt32(b)) => a.eq(b),
            (ValueRef::UInt64(a), ValueRef::UInt64(b)) => a.eq(b),
            (ValueRef::Float32(a), ValueRef::Float32(b)) => a.to_bits() == b.to_bits(),
            (ValueRef::Float64(a), ValueRef::Float64(b)) => a.to_bits() == b.to_bits(),
            (ValueRef::String(a), ValueRef::String(b)) => a.eq(b),
            (ValueRef::Binary(a), ValueRef::Binary(b)) => a.eq(b),
            (ValueRef::Date32(a), ValueRef::Date32(b)) => a.eq(b),
            (ValueRef::Date64(a), ValueRef::Date64(b)) => a.eq(b),
            (ValueRef::Timestamp(a, unit1), ValueRef::Timestamp(b, unit2)) => {
                if unit1 == unit2 {
                    return a.eq(b);
                }
                let (s_sec, s_nsec) = split_second_ns(*a, *unit1);
                let (o_sec, o_nsec) = split_second_ns(*b, *unit2);
                s_sec == o_sec && s_nsec == o_nsec
            }
            (ValueRef::Time32(a, unit1), ValueRef::Time32(b, unit2)) => {
                if unit1 == unit2 {
                    return a.eq(b);
                }
                let (s_sec, s_nsec) = split_second_ns(*a as i64, *unit1);
                let (o_sec, o_nsec) = split_second_ns(*b as i64, *unit2);
                s_sec == o_sec && s_nsec == o_nsec
            }
            (ValueRef::Time64(a, unit1), ValueRef::Time64(b, unit2)) => {
                if unit1 == unit2 {
                    return a.eq(b);
                }
                let (s_sec, s_nsec) = split_second_ns(*a, *unit1);
                let (o_sec, o_nsec) = split_second_ns(*b, *unit2);
                s_sec == o_sec && s_nsec == o_nsec
            }
            _ => {
                panic!("can not compare different types: {self:?} and {other:?}")
            }
        }
    }
}

impl PartialOrd for ValueRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValueRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ValueRef::Null, ValueRef::Null) => Ordering::Equal,
            (ValueRef::Null, _) => Ordering::Less,
            (_, ValueRef::Null) => Ordering::Greater,
            (ValueRef::Boolean(a), ValueRef::Boolean(b)) => a.cmp(b),
            (ValueRef::Int8(a), ValueRef::Int8(b)) => a.cmp(b),
            (ValueRef::Int16(a), ValueRef::Int16(b)) => a.cmp(b),
            (ValueRef::Int32(a), ValueRef::Int32(b)) => a.cmp(b),
            (ValueRef::Int64(a), ValueRef::Int64(b)) => a.cmp(b),
            (ValueRef::UInt8(a), ValueRef::UInt8(b)) => a.cmp(b),
            (ValueRef::UInt16(a), ValueRef::UInt16(b)) => a.cmp(b),
            (ValueRef::UInt32(a), ValueRef::UInt32(b)) => a.cmp(b),
            (ValueRef::UInt64(a), ValueRef::UInt64(b)) => a.cmp(b),
            (ValueRef::Float32(a), ValueRef::Float32(b)) => a.total_cmp(b),
            (ValueRef::Float64(a), ValueRef::Float64(b)) => a.total_cmp(b),
            (ValueRef::String(a), ValueRef::String(b)) => a.cmp(b),
            (ValueRef::Binary(a), ValueRef::Binary(b)) => a.cmp(b),
            (ValueRef::Date32(a), ValueRef::Date32(b)) => a.cmp(b),
            (ValueRef::Date64(a), ValueRef::Date64(b)) => a.cmp(b),
            (ValueRef::Timestamp(a, unit1), ValueRef::Timestamp(b, unit2)) => {
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
                panic!("can not compare different types: {self:?} and {other:?}")
            }
        }
    }
}
impl<'a> KeyRef<'a> for ValueRef<'a> {
    type Key = Value;

    fn to_key(self) -> Self::Key {
        match self {
            ValueRef::Null => Value::Null,
            ValueRef::Boolean(v) => Value::Boolean(v),
            ValueRef::Int8(v) => Value::Int8(v),
            ValueRef::Int16(v) => Value::Int16(v),
            ValueRef::Int32(v) => Value::Int32(v),
            ValueRef::Int64(v) => Value::Int64(v),
            ValueRef::UInt8(v) => Value::UInt8(v),
            ValueRef::UInt16(v) => Value::UInt16(v),
            ValueRef::UInt32(v) => Value::UInt32(v),
            ValueRef::UInt64(v) => Value::UInt64(v),
            ValueRef::Float32(v) => Value::Float32(v),
            ValueRef::Float64(v) => Value::Float64(v),
            ValueRef::String(v) => Value::String(v.to_string()),
            ValueRef::Binary(v) => Value::Binary(v.to_vec()),
            ValueRef::Date32(v) => Value::Date32(v),
            ValueRef::Date64(v) => Value::Date64(v),
            ValueRef::Timestamp(v, time_unit) => Value::Timestamp(v, time_unit),
            ValueRef::Time32(v, time_unit) => Value::Time32(v, time_unit),
            ValueRef::Time64(v, time_unit) => Value::Time64(v, time_unit),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int32Array, StringArray, TimestampMillisecondArray},
        datatypes::DataType,
    };

    use crate::record::{AsValue, TimeUnit, ValueRef};

    #[test]
    fn test_value_ref_basic_types() {
        assert_eq!(ValueRef::Null.data_type(), DataType::Null);
        assert_eq!(ValueRef::Boolean(true).data_type(), DataType::Boolean);
        assert_eq!(ValueRef::Int32(42).data_type(), DataType::Int32);
        assert_eq!(ValueRef::Int64(42).data_type(), DataType::Int64);
        assert_eq!(
            ValueRef::Float32(std::f32::consts::PI).data_type(),
            DataType::Float32
        );
        assert_eq!(
            ValueRef::Float64(std::f64::consts::PI).data_type(),
            DataType::Float64
        );
        assert_eq!(ValueRef::String("hello").data_type(), DataType::Utf8);
        assert_eq!(ValueRef::Binary(&[1, 2, 3]).data_type(), DataType::Binary);
    }

    #[test]
    fn test_value_ref_null_checks() {
        assert!(ValueRef::Null.is_null());
        assert!(!ValueRef::Boolean(false).is_null());
        assert!(!ValueRef::Int32(0).is_null());
        assert!(!ValueRef::String("hello").is_null());
        assert!(!ValueRef::Binary(&[1, 2, 3]).is_null());
    }

    #[test]
    fn test_value_ref_conversions() {
        assert!(ValueRef::Boolean(true).as_bool());
        assert_eq!(*ValueRef::Int8(42).as_i8(), 42i8);
        assert_eq!(*ValueRef::Int32(42).as_i32(), 42i32);
        assert!(
            (ValueRef::Float64(std::f64::consts::PI).as_f64() - std::f64::consts::PI).abs()
                < 0.0001
        );
        assert_eq!(ValueRef::String("hello").as_string(), "hello");
        assert_eq!(ValueRef::Binary(&[1, 2, 3]).as_bytes(), &[1, 2, 3]);
    }

    #[test]
    fn test_value_ref_conversion_fail() {
        assert!(ValueRef::Int32(42).as_bool_opt().is_none());
        assert!(ValueRef::String("hello").as_i64_opt().is_none());
        assert!(ValueRef::Boolean(true).as_string_opt().is_none());
        assert!(ValueRef::Float32(1f32).as_i32_opt().is_none());
    }

    #[test]
    fn test_float_value_ref_cmp() {
        // test zero
        {
            let zero = ValueRef::Float32(0.0_f32);
            let neg_zero = ValueRef::Float32(-0.0_f32);
            assert!(zero > neg_zero);
        }
        // test NAN and INF
        {
            let nan1 = ValueRef::Float32(f32::NAN);
            let nan2 = ValueRef::Float32(f32::NAN);
            let neg_nan = ValueRef::Float32(-f32::NAN);
            let inf = ValueRef::Float32(f32::INFINITY);
            let neg_inf = ValueRef::Float32(f32::NEG_INFINITY);

            // This is not consistent with the IEEE, but it's consistent with the Arrow

            assert_eq!(nan1, nan2);
            assert!(nan1 > neg_nan);
            // negative NAN should be less than negative infinity
            assert!(neg_nan < neg_inf);
            // positive NAN should be greater than positive infinity
            assert!(nan1 > inf);
        }
        {
            let f1 = ValueRef::Float32(1.0_f32);
            let f2 = ValueRef::Float32(2.1_f32);
            let f3 = ValueRef::Float32(2.1_f32);
            assert!(f1 < f2);
            assert!(f2 == f3);
        }
    }

    #[test]
    fn test_timestamp_value_ref_cmp() {
        let t1 = ValueRef::Timestamp(1716, TimeUnit::Second);
        let t2 = ValueRef::Timestamp(1716000, TimeUnit::Millisecond);
        let t3 = ValueRef::Timestamp(1716000001, TimeUnit::Microsecond);
        let t4 = ValueRef::Timestamp(1715999999999, TimeUnit::Nanosecond);
        assert!(t1 == t2);
        assert!(t1 < t3);
        assert!(t1 > t4);
    }

    #[test]
    fn test_value_ref_from_array_ref() {
        {
            let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
            assert_eq!(
                ValueRef::from_array_ref(&array, 0).unwrap(),
                ValueRef::Int32(1)
            );
            assert_eq!(
                ValueRef::from_array_ref(&array, 1).unwrap(),
                ValueRef::Int32(2)
            );
        }
        {
            let array = Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef;
            assert_eq!(
                ValueRef::from_array_ref(&array, 0).unwrap(),
                ValueRef::String("1")
            );
            assert_eq!(
                ValueRef::from_array_ref(&array, 1).unwrap(),
                ValueRef::String("2")
            );
        }
        {
            let array = Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3])) as ArrayRef;
            assert_eq!(
                ValueRef::from_array_ref(&array, 0).unwrap(),
                ValueRef::Timestamp(1, TimeUnit::Millisecond)
            );
            assert_eq!(
                ValueRef::from_array_ref(&array, 1).unwrap(),
                ValueRef::Timestamp(2, TimeUnit::Millisecond)
            );
        }
    }
}
