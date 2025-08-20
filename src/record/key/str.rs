use std::sync::Arc;

use arrow::array::{Datum, StringArray};

use super::{Key, KeyRef};

pub type LargeString = String;

impl Key for String {
    type Ref<'r> = &'r str;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        self
    }

    fn to_arrow_datums(&self) -> Vec<Arc<dyn Datum>> {
        vec![Arc::new(StringArray::new_scalar(self)) as Arc<dyn Datum>]
    }
}

impl<'r> KeyRef<'r> for &'r str {
    type Key = String;

    fn to_key(self) -> Self::Key {
        self.to_string()
    }
}
