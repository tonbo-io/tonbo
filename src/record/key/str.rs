use std::sync::Arc;

use arrow::array::{Datum, StringArray};

use super::{Key, KeyRef};

impl Key for String {
    type Ref<'r> = &'r str;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        self
    }

    fn to_arrow_datum(&self) -> Arc<dyn Datum> {
        Arc::new(StringArray::new_scalar(self))
    }

    fn as_i32(&self) -> i32 {
        panic!("String can not be casted to i32")
    }

    fn as_i64(&self) -> i64 {
        panic!("String can not be casted to i64")
    }

    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'r> KeyRef<'r> for &'r str {
    type Key = String;

    fn to_key(self) -> Self::Key {
        self.to_string()
    }
}
