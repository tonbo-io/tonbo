use std::{any::Any, hash::Hash, ops::Deref, sync::Arc};

use arrow::array::{
    Datum, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use fusio::{SeqRead, Write};
use fusio_log::{Decode, Encode};

use crate::{
    datatype::DataType,
    key::{Key, KeyRef},
    PrimaryKey, Value, ValueRef,
};

#[macro_export]
macro_rules! implement_key {
    ($struct_name:ident, $array_name:ident, $data_type:expr) => {
        impl Key for $struct_name {
            type Ref<'r> = $struct_name;

            fn as_key_ref(&self) -> Self::Ref<'_> {
                *self
            }

            fn as_value(&self) -> &dyn Value {
                self
            }
        }

        impl<'a> KeyRef<'a> for $struct_name {
            type Key = $struct_name;

            fn to_key(self) -> Self::Key {
                self
            }
        }

        impl Value for $struct_name {
            fn data_type(&self) -> DataType {
                $data_type
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn is_none(&self) -> bool {
                false
            }

            fn is_some(&self) -> bool {
                false
            }

            fn clone_arc(&self) -> ValueRef {
                Arc::new(*self)
            }

            fn to_arrow_datum(&self) -> Option<Arc<dyn Datum>> {
                Some(Arc::new($array_name::new_scalar(*self)))
            }
        }

        impl Value for Option<$struct_name> {
            fn data_type(&self) -> DataType {
                $data_type
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn is_none(&self) -> bool {
                self.is_none()
            }

            fn is_some(&self) -> bool {
                self.is_some()
            }

            fn clone_arc(&self) -> ValueRef {
                Arc::new(*self)
            }

            fn to_arrow_datum(&self) -> Option<Arc<dyn Datum>> {
                None
            }
        }

        impl From<$struct_name> for PrimaryKey {
            fn from(value: $struct_name) -> Self {
                PrimaryKey::new(vec![Arc::new(value)])
            }
        }
    };
}

implement_key!(i8, Int8Array, DataType::Int8);
implement_key!(i16, Int16Array, DataType::Int16);
implement_key!(i32, Int32Array, DataType::Int32);
implement_key!(i64, Int64Array, DataType::Int64);
implement_key!(u8, UInt8Array, DataType::UInt8);
implement_key!(u16, UInt16Array, DataType::UInt16);
implement_key!(u32, UInt32Array, DataType::UInt32);
implement_key!(u64, UInt64Array, DataType::UInt64);

#[derive(Debug, Default, Clone, Copy)]
pub struct FloatType<T>(pub T);

pub type F32 = FloatType<f32>;
pub type F64 = FloatType<f64>;

#[macro_export]
macro_rules! implement_float_encode_decode {
    ($ty:ident) => {
        impl Encode for FloatType<$ty> {
            async fn encode<W: Write>(&self, writer: &mut W) -> Result<(), fusio::Error> {
                let (result, _) = writer.write_all(&self.to_le_bytes()[..]).await;
                result?;

                Ok(())
            }

            fn size(&self) -> usize {
                size_of::<Self>()
            }
        }

        impl Decode for FloatType<$ty> {
            async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, fusio::Error> {
                let mut bytes = [0u8; size_of::<Self>()];
                let (result, _) = reader.read_exact(&mut bytes[..]).await;
                result?;

                Ok(FloatType::<$ty>::from($ty::from_le_bytes(bytes)))
            }
        }
    };
}

#[macro_export]
macro_rules! implement_float_key {
    ($ty:ty, $array_name:ident, $data_type:expr) => {
        impl Ord for FloatType<$ty> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.total_cmp(&other.0)
            }
        }

        impl PartialEq for FloatType<$ty> {
            fn eq(&self, other: &Self) -> bool {
                self.0.to_bits() == other.0.to_bits()
            }
        }

        impl PartialOrd for FloatType<$ty> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Eq for FloatType<$ty> {}

        impl Hash for FloatType<$ty> {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                state.write(&<$ty>::from_le_bytes(self.0.to_le_bytes()).to_le_bytes())
            }
        }

        impl From<$ty> for FloatType<$ty> {
            fn from(value: $ty) -> Self {
                Self(value)
            }
        }

        impl From<FloatType<$ty>> for $ty {
            fn from(value: FloatType<$ty>) -> Self {
                value.0
            }
        }

        impl From<&FloatType<$ty>> for $ty {
            fn from(value: &FloatType<$ty>) -> Self {
                value.0
            }
        }

        impl Deref for FloatType<$ty> {
            type Target = $ty;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl Key for FloatType<$ty> {
            type Ref<'r> = FloatType<$ty>;

            fn as_key_ref(&self) -> Self::Ref<'_> {
                *self
            }

            fn as_value(&self) -> &dyn Value {
                self
            }
        }

        impl<'a> KeyRef<'a> for FloatType<$ty> {
            type Key = FloatType<$ty>;

            fn to_key(self) -> Self::Key {
                self
            }
        }

        impl FloatType<$ty> {
            pub fn value(&self) -> $ty {
                self.0
            }
        }

        impl Value for FloatType<$ty> {
            fn data_type(&self) -> DataType {
                $data_type
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn to_arrow_datum(&self) -> Option<Arc<dyn Datum>> {
                Some(Arc::new($array_name::new_scalar(self.0)))
            }

            fn is_none(&self) -> bool {
                false
            }

            fn is_some(&self) -> bool {
                false
            }

            fn clone_arc(&self) -> ValueRef {
                Arc::new(*self)
            }
        }

        impl Value for Option<FloatType<$ty>> {
            fn data_type(&self) -> DataType {
                $data_type
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn is_none(&self) -> bool {
                self.is_none()
            }

            fn is_some(&self) -> bool {
                self.is_some()
            }

            fn clone_arc(&self) -> ValueRef {
                Arc::new(*self)
            }

            fn to_arrow_datum(&self) -> Option<Arc<dyn Datum>> {
                None
            }
        }

        impl From<$ty> for PrimaryKey {
            fn from(value: $ty) -> Self {
                PrimaryKey::new(vec![Arc::new(FloatType::<$ty>(value))])
            }
        }
    };
}

implement_float_encode_decode!(f32);
implement_float_encode_decode!(f64);

implement_float_key!(f32, Float32Array, DataType::Float32);
implement_float_key!(f64, Float64Array, DataType::Float64);

#[cfg(test)]
mod tests {
    use core::f32;

    use arrow::array::ArrowNativeTypeOp;

    use crate::key::num::F32;

    #[tokio::test]
    async fn test_zero() {
        let f1 = F32::from(0_f32);
        let f2 = F32::from(-0_f32);
        // +0 should be greater than -0
        assert_eq!(f1.cmp(&f2), f1.0.compare(f2.0));
        assert!(f1 > f2);
    }

    #[tokio::test]
    async fn test_eq() {
        let f1 = F32::from(1.01_f32);
        let f2 = F32::from(1.01_f32);
        assert!(f1 == f2);
    }

    #[tokio::test]
    async fn test_nan_cmp() {
        let f1 = F32::from(f32::NAN);
        let f2 = F32::from(f32::NAN);
        let f3 = F32::from(-f32::NAN);
        let inf = F32::from(f32::INFINITY);
        let neg_inf = F32::from(f32::NEG_INFINITY);

        // This is not consistent with the IEEE
        // assert_eq!(f32::NAN, f32::NAN);
        assert_eq!(f1.cmp(&f2), f1.0.compare(f2.0));
        assert_eq!(f1, f2);

        assert_eq!(f1.cmp(&f3), f1.0.compare(f3.0));
        assert_eq!(f1, f2);

        // positive NAN should be greater than positive infinity
        assert_eq!(f1.cmp(&inf), f1.0.compare(inf.0));
        assert!(f1 > inf);

        // negative NAN should be less than negative infinity
        assert_eq!(f3.cmp(&neg_inf), f3.0.compare(neg_inf.0));
        assert!(f3 < neg_inf);

        let f4 = F32::from(1.0_f32);
        assert_eq!(f1.cmp(&f4), f1.0.compare(f4.0));
        assert!(f1 > f4);

        assert_eq!(f3.cmp(&f4), f3.0.compare(f4.0));
        assert!(f3 < f4);
    }
}
