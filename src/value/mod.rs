pub mod conversion;

use std::{cmp::Ordering, fmt, sync::Arc};

use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ValueError {
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
    #[error("Invalid conversion: {0}")]
    InvalidConversion(String),
    #[error("Null value not allowed")]
    NullNotAllowed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
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
    Date32(i32),
    Date64(i64),
    Timestamp(i64, TimeUnit),
    List(Vec<Value>),
    Struct(Vec<(String, Value)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Value {
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
            Value::List(values) => {
                if values.is_empty() {
                    DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::Null,
                        true,
                    )))
                } else {
                    DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        values[0].data_type(),
                        true,
                    )))
                }
            }
            Value::Struct(fields) => {
                let arrow_fields: Vec<arrow::datatypes::Field> = fields
                    .iter()
                    .map(|(name, value)| {
                        arrow::datatypes::Field::new(name, value.data_type(), true)
                    })
                    .collect();
                DataType::Struct(arrow_fields.into())
            }
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_bool(&self) -> Result<bool, ValueError> {
        match self {
            Value::Boolean(v) => Ok(*v),
            _ => Err(ValueError::TypeMismatch {
                expected: "Boolean".to_string(),
                actual: format!("{self:?}"),
            }),
        }
    }

    pub fn as_i64(&self) -> Result<i64, ValueError> {
        match self {
            Value::Int8(v) => Ok(*v as i64),
            Value::Int16(v) => Ok(*v as i64),
            Value::Int32(v) => Ok(*v as i64),
            Value::Int64(v) => Ok(*v),
            _ => Err(ValueError::TypeMismatch {
                expected: "Integer".to_string(),
                actual: format!("{self:?}"),
            }),
        }
    }

    pub fn as_f64(&self) -> Result<f64, ValueError> {
        match self {
            Value::Float32(v) => Ok(*v as f64),
            Value::Float64(v) => Ok(*v),
            Value::Int8(v) => Ok(*v as f64),
            Value::Int16(v) => Ok(*v as f64),
            Value::Int32(v) => Ok(*v as f64),
            Value::Int64(v) => Ok(*v as f64),
            _ => Err(ValueError::TypeMismatch {
                expected: "Float".to_string(),
                actual: format!("{self:?}"),
            }),
        }
    }

    pub fn as_string(&self) -> Result<&str, ValueError> {
        match self {
            Value::String(v) => Ok(v),
            _ => Err(ValueError::TypeMismatch {
                expected: "String".to_string(),
                actual: format!("{self:?}"),
            }),
        }
    }

    pub fn as_binary(&self) -> Result<&[u8], ValueError> {
        match self {
            Value::Binary(v) => Ok(v),
            _ => Err(ValueError::TypeMismatch {
                expected: "Binary".to_string(),
                actual: format!("{self:?}"),
            }),
        }
    }
}

impl Eq for Value {}

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
            (Value::Float32(a), Value::Float32(b)) => {
                // Handle NaN values for total ordering
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) => a.partial_cmp(b).unwrap(),
                }
            }
            (Value::Float64(a), Value::Float64(b)) => {
                // Handle NaN values for total ordering
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) => a.partial_cmp(b).unwrap(),
                }
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Binary(a), Value::Binary(b)) => a.cmp(b),
            (Value::Date32(a), Value::Date32(b)) => a.cmp(b),
            (Value::Date64(a), Value::Date64(b)) => a.cmp(b),
            (Value::Timestamp(a, _), Value::Timestamp(b, _)) => a.cmp(b),
            // Different types - order by variant index
            _ => {
                // Define a consistent ordering for different types
                fn type_order(value: &Value) -> u8 {
                    match value {
                        Value::Null => 0,
                        Value::Boolean(_) => 1,
                        Value::Int8(_) => 2,
                        Value::Int16(_) => 3,
                        Value::Int32(_) => 4,
                        Value::Int64(_) => 5,
                        Value::UInt8(_) => 6,
                        Value::UInt16(_) => 7,
                        Value::UInt32(_) => 8,
                        Value::UInt64(_) => 9,
                        Value::Float32(_) => 10,
                        Value::Float64(_) => 11,
                        Value::String(_) => 12,
                        Value::Binary(_) => 13,
                        Value::Date32(_) => 14,
                        Value::Date64(_) => 15,
                        Value::Timestamp(_, _) => 16,
                        Value::List(_) => 17,
                        Value::Struct(_) => 18,
                    }
                }
                type_order(self).cmp(&type_order(other))
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
            Value::Date32(v) => write!(f, "Date32({v})"),
            Value::Date64(v) => write!(f, "Date64({v})"),
            Value::Timestamp(v, unit) => write!(f, "Timestamp({v}, {unit:?})"),
            Value::List(values) => {
                write!(f, "[")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Value::Struct(fields) => {
                write!(f, "{{")?;
                for (i, (name, value)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{name}: {value}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl From<TimeUnit> for arrow::datatypes::TimeUnit {
    fn from(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
            TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
            TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
        }
    }
}

impl From<arrow::datatypes::TimeUnit> for TimeUnit {
    fn from(unit: arrow::datatypes::TimeUnit) -> Self {
        match unit {
            arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
            arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
            arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
            arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

/// A reference type for Value that avoids cloning
#[derive(Debug, Clone, Copy, PartialEq)]
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
}

impl<'a> ValueRef<'a> {
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
            Value::List(_) | Value::Struct(_) => {
                panic!("Complex types not supported in ValueRef")
            }
        }
    }
}

impl<'a> Eq for ValueRef<'a> {}

impl<'a> PartialOrd for ValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for ValueRef<'a> {
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
            (ValueRef::Float32(a), ValueRef::Float32(b)) => {
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) => a.partial_cmp(b).unwrap(),
                }
            }
            (ValueRef::Float64(a), ValueRef::Float64(b)) => {
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) => a.partial_cmp(b).unwrap(),
                }
            }
            (ValueRef::String(a), ValueRef::String(b)) => a.cmp(b),
            (ValueRef::Binary(a), ValueRef::Binary(b)) => a.cmp(b),
            (ValueRef::Date32(a), ValueRef::Date32(b)) => a.cmp(b),
            (ValueRef::Date64(a), ValueRef::Date64(b)) => a.cmp(b),
            (ValueRef::Timestamp(a, _), ValueRef::Timestamp(b, _)) => a.cmp(b),
            _ => {
                fn type_order(value: &ValueRef) -> u8 {
                    match value {
                        ValueRef::Null => 0,
                        ValueRef::Boolean(_) => 1,
                        ValueRef::Int8(_) => 2,
                        ValueRef::Int16(_) => 3,
                        ValueRef::Int32(_) => 4,
                        ValueRef::Int64(_) => 5,
                        ValueRef::UInt8(_) => 6,
                        ValueRef::UInt16(_) => 7,
                        ValueRef::UInt32(_) => 8,
                        ValueRef::UInt64(_) => 9,
                        ValueRef::Float32(_) => 10,
                        ValueRef::Float64(_) => 11,
                        ValueRef::String(_) => 12,
                        ValueRef::Binary(_) => 13,
                        ValueRef::Date32(_) => 14,
                        ValueRef::Date64(_) => 15,
                        ValueRef::Timestamp(_, _) => 16,
                    }
                }
                type_order(self).cmp(&type_order(other))
            }
        }
    }
}

#[cfg(test)]
mod tests;
