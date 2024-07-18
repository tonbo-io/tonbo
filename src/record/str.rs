use std::sync::Arc;

use arrow::{
    array::{
        Array, AsArray, BooleanArray, BooleanBufferBuilder, Datum, RecordBatch, StringArray,
        StringBuilder, UInt32Array, UInt32Builder,
    },
    datatypes::{DataType, Field, Schema, UInt32Type},
};
use once_cell::sync::Lazy;

use super::{internal::InternalRecordRef, Key, KeyRef, Record, RecordRef};
use crate::{
    inmem::immutable::{ArrowArrays, Builder},
    oracle::timestamp::Timestamped,
};

impl Key for String {
    type Ref<'r> = &'r str;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        self
    }

    fn to_arrow_datum(&self) -> impl Datum {
        StringArray::new_scalar(self)
    }
}

impl<'r> KeyRef<'r> for &'r str {
    type Key = String;

    fn to_key(&self) -> Self::Key {
        self.to_string()
    }
}

impl Record for String {
    type Columns = StringColumns;

    type Key = Self;

    type Ref<'r> = &'r str
    where
        Self: 'r;

    fn key(&self) -> &str {
        self
    }

    fn as_record_ref(&self) -> Self::Ref<'_> {
        self
    }

    fn arrow_schema() -> &'static Arc<Schema> {
        static SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
            Arc::new(Schema::new(vec![
                Field::new("_null", DataType::Boolean, false),
                Field::new("_ts", DataType::UInt32, false),
                Field::new("vstring", DataType::Utf8, false),
            ]))
        });

        &SCHEMA
    }
}

impl<'r> RecordRef<'r> for &'r str {
    type Record = String;

    fn key(self) -> <<Self::Record as Record>::Key as Key>::Ref<'r> {
        self
    }

    fn from_record_batch(record_batch: &'r RecordBatch, offset: usize) -> InternalRecordRef<Self> {
        let ts = record_batch
            .column(1)
            .as_primitive::<UInt32Type>()
            .value(offset)
            .into();
        let vstring = record_batch.column(2).as_string::<i32>().value(offset);
        let null = record_batch.column(0).as_boolean().value(offset);

        InternalRecordRef::new(ts, vstring, null)
    }
}

#[derive(Debug)]
pub struct StringColumns {
    _null: Arc<BooleanArray>,
    _ts: Arc<UInt32Array>,
    string: Arc<StringArray>,

    record_batch: RecordBatch,
}

impl ArrowArrays for StringColumns {
    type Record = String;

    type Builder = StringColumnsBuilder;

    fn builder(capacity: usize) -> Self::Builder {
        StringColumnsBuilder {
            _null: BooleanBufferBuilder::new(capacity),
            _ts: UInt32Builder::with_capacity(capacity),
            string: StringBuilder::with_capacity(capacity, 0),
        }
    }

    fn get(&self, offset: u32) -> Option<Option<<Self::Record as Record>::Ref<'_>>> {
        if self._null.value(offset as usize) {
            return Some(None);
        }

        Some(Some(self.string.value(offset as usize)))
    }

    fn as_record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }
}

#[derive(Debug)]
pub struct StringColumnsBuilder {
    _null: BooleanBufferBuilder,
    _ts: UInt32Builder,
    string: StringBuilder,
}

impl Builder<StringColumns> for StringColumnsBuilder {
    fn push(&mut self, key: Timestamped<&str>, row: Option<&str>) {
        self._null.append(row.is_none());
        self._ts.append_value(key.ts.into());
        if let Some(row) = row {
            self.string.append_value(row);
        } else {
            self.string.append_value(String::default());
        }
    }

    fn finish(&mut self) -> StringColumns {
        let _null = Arc::new(BooleanArray::new(self._null.finish(), None));
        let _ts = Arc::new(self._ts.finish());
        let string = Arc::new(self.string.finish());

        let record_batch = RecordBatch::try_new(
            <StringColumns as ArrowArrays>::Record::arrow_schema().clone(),
            vec![
                Arc::clone(&_null) as Arc<dyn Array>,
                Arc::clone(&_ts) as Arc<dyn Array>,
                Arc::clone(&string) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        StringColumns {
            _null,
            _ts,
            string,
            record_batch,
        }
    }
}
