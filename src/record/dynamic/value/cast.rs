use crate::record::{Value, ValueRef};

mod seal {
    pub trait Sealed {}
}

impl seal::Sealed for Value {}

impl seal::Sealed for ValueRef<'_> {}

/// A conversion trait for fast access to the value of a [`Value`] or [`ValueRef`]
pub trait AsValue: seal::Sealed {
    fn as_bool_opt(&self) -> Option<&bool>;

    fn as_bool(&self) -> &bool {
        self.as_bool_opt().expect("bool")
    }

    fn as_u8_opt(&self) -> Option<&u8>;

    fn as_u8(&self) -> &u8 {
        self.as_u8_opt().expect("u8")
    }

    fn as_u16_opt(&self) -> Option<&u16>;

    fn as_u16(&self) -> &u16 {
        self.as_u16_opt().expect("u16")
    }

    fn as_u32_opt(&self) -> Option<&u32>;

    fn as_u32(&self) -> &u32 {
        self.as_u32_opt().expect("u32")
    }

    fn as_u64_opt(&self) -> Option<&u64>;

    fn as_u64(&self) -> &u64 {
        self.as_u64_opt().expect("u64")
    }

    fn as_i8_opt(&self) -> Option<&i8>;

    fn as_i8(&self) -> &i8 {
        self.as_i8_opt().expect("i8")
    }

    fn as_i16_opt(&self) -> Option<&i16>;

    fn as_i16(&self) -> &i16 {
        self.as_i16_opt().expect("i16")
    }

    fn as_i32_opt(&self) -> Option<&i32>;

    fn as_i32(&self) -> &i32 {
        self.as_i32_opt().expect("i32")
    }

    fn as_i64_opt(&self) -> Option<&i64>;

    fn as_i64(&self) -> &i64 {
        self.as_i64_opt().expect("i64")
    }

    fn as_f32_opt(&self) -> Option<&f32>;

    fn as_f32(&self) -> &f32 {
        self.as_f32_opt().expect("f32")
    }

    fn as_f64_opt(&self) -> Option<&f64>;

    fn as_f64(&self) -> &f64 {
        self.as_f64_opt().expect("f64")
    }

    fn as_bytes_opt(&self) -> Option<&[u8]>;

    fn as_bytes(&self) -> &[u8] {
        self.as_bytes_opt().expect("bytes")
    }

    fn as_string_opt(&self) -> Option<&str>;

    fn as_string(&self) -> &str {
        self.as_string_opt().expect("string")
    }
}

impl AsValue for Value {
    fn as_bool_opt(&self) -> Option<&bool> {
        match self {
            Value::Boolean(v) => Some(v),
            _ => None,
        }
    }

    fn as_u8_opt(&self) -> Option<&u8> {
        match self {
            Value::UInt8(v) => Some(v),
            _ => None,
        }
    }

    fn as_u16_opt(&self) -> Option<&u16> {
        match self {
            Value::UInt16(v) => Some(v),
            _ => None,
        }
    }

    fn as_u32_opt(&self) -> Option<&u32> {
        match self {
            Value::UInt32(v) => Some(v),
            _ => None,
        }
    }

    fn as_u64_opt(&self) -> Option<&u64> {
        match self {
            Value::UInt64(v) => Some(v),
            _ => None,
        }
    }

    fn as_i8_opt(&self) -> Option<&i8> {
        match self {
            Value::Int8(v) => Some(v),
            _ => None,
        }
    }

    fn as_i16_opt(&self) -> Option<&i16> {
        match self {
            Value::Int16(v) => Some(v),
            _ => None,
        }
    }

    fn as_i32_opt(&self) -> Option<&i32> {
        match self {
            Value::Int32(v) => Some(v),
            _ => None,
        }
    }

    fn as_i64_opt(&self) -> Option<&i64> {
        match self {
            Value::Int64(v) => Some(v),
            _ => None,
        }
    }

    fn as_f32_opt(&self) -> Option<&f32> {
        match self {
            Value::Float32(v) => Some(v),
            _ => None,
        }
    }

    fn as_f64_opt(&self) -> Option<&f64> {
        match self {
            Value::Float64(v) => Some(v),
            _ => None,
        }
    }

    fn as_bytes_opt(&self) -> Option<&[u8]> {
        match self {
            Value::Binary(v) => Some(v),
            Value::FixedSizeBinary(v, _) => Some(v),
            _ => None,
        }
    }

    fn as_string_opt(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }
}

impl AsValue for ValueRef<'_> {
    fn as_bool_opt(&self) -> Option<&bool> {
        match self {
            ValueRef::Boolean(v) => Some(v),
            _ => None,
        }
    }

    fn as_u8_opt(&self) -> Option<&u8> {
        match self {
            ValueRef::UInt8(v) => Some(v),
            _ => None,
        }
    }

    fn as_u16_opt(&self) -> Option<&u16> {
        match self {
            ValueRef::UInt16(v) => Some(v),
            _ => None,
        }
    }

    fn as_u32_opt(&self) -> Option<&u32> {
        match self {
            ValueRef::UInt32(v) => Some(v),
            _ => None,
        }
    }

    fn as_u64_opt(&self) -> Option<&u64> {
        match self {
            ValueRef::UInt64(v) => Some(v),
            _ => None,
        }
    }

    fn as_i8_opt(&self) -> Option<&i8> {
        match self {
            ValueRef::Int8(v) => Some(v),
            _ => None,
        }
    }

    fn as_i16_opt(&self) -> Option<&i16> {
        match self {
            ValueRef::Int16(v) => Some(v),
            _ => None,
        }
    }

    /// Return the value as i32 if it is a i32, time32, date32 or timestamp
    ///
    /// Note: only value will be returned and time unit will be ignored
    fn as_i32_opt(&self) -> Option<&i32> {
        match self {
            ValueRef::Int32(v) => Some(v),
            ValueRef::Time32(v, _) => Some(v),
            ValueRef::Date32(v) => Some(v),
            _ => None,
        }
    }

    /// Return the value as i64 if it is a i64, time64, date64 or timestamp
    ///
    /// Note: only value will be returned and time unit will be ignored
    fn as_i64_opt(&self) -> Option<&i64> {
        match self {
            ValueRef::Int64(v) => Some(v),
            ValueRef::Time64(v, _) => Some(v),
            ValueRef::Date64(v) => Some(v),
            ValueRef::Timestamp(v, _) => Some(v),
            _ => None,
        }
    }

    fn as_f32_opt(&self) -> Option<&f32> {
        match self {
            ValueRef::Float32(v) => Some(v),
            _ => None,
        }
    }

    fn as_f64_opt(&self) -> Option<&f64> {
        match self {
            ValueRef::Float64(v) => Some(v),
            _ => None,
        }
    }

    fn as_bytes_opt(&self) -> Option<&[u8]> {
        match self {
            ValueRef::Binary(v) => Some(v),
            ValueRef::FixedSizeBinary(v, _) => Some(v),
            _ => None,
        }
    }

    fn as_string_opt(&self) -> Option<&str> {
        match self {
            ValueRef::String(v) => Some(v),
            _ => None,
        }
    }
}
