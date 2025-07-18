use std::sync::Arc;

use arrow::array::{Datum, StringArray};

use super::{Key, KeyRef};

pub type LargeString = String;

impl Key for String {
    type Ref<'r> = &'r str;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        self
    }

    fn to_arrow_datum(&self) -> Arc<dyn Datum> {
        Arc::new(StringArray::new_scalar(self))
    }
}

impl<'r> KeyRef<'r> for &'r str {
    type Key = String;

    fn to_key(self) -> Self::Key {
        self.to_string()
    }
}
