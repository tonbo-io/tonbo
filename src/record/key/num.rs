use std::sync::Arc;

use arrow::array::{
    Datum, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};

use crate::record::{Key, KeyRef};

#[macro_export]
macro_rules! implement_key {
    ($struct_name:ident, $array_name:ident) => {
        impl Key for $struct_name {
            type Ref<'r> = $struct_name;

            fn as_key_ref(&self) -> Self::Ref<'_> {
                *self
            }

            fn to_arrow_datum(&self) -> Arc<dyn Datum> {
                Arc::new($array_name::new_scalar(*self))
            }
        }

        impl<'a> KeyRef<'a> for $struct_name {
            type Key = $struct_name;

            fn to_key(self) -> Self::Key {
                self
            }
        }
    };
}

implement_key!(i8, Int8Array);
implement_key!(i16, Int16Array);
implement_key!(i32, Int32Array);
implement_key!(i64, Int64Array);
implement_key!(u8, UInt8Array);
implement_key!(u16, UInt16Array);
implement_key!(u32, UInt32Array);
implement_key!(u64, UInt64Array);
