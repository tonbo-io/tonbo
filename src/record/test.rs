use std::{mem, string::ToString, sync::Arc};

use arrow::{
    array::{
        Array, AsArray, BooleanArray, BooleanBufferBuilder, RecordBatch, StringArray,
        StringBuilder, UInt32Array, UInt32Builder,
    },
    datatypes::{DataType, Field, Schema as ArrowSchema, UInt32Type},
};
use once_cell::sync::Lazy;
use parquet::{arrow::ProjectionMask, format::SortingColumn, schema::types::ColumnPath};

use super::{
    option::OptionRecordRef, ArrowArrays, ArrowArraysBuilder, Key, Record, RecordRef, Schema,
};
#[cfg(all(test, feature = "tokio"))]
use crate::inmem::immutable::tests::TestSchema;
use crate::{magic, version::timestamp::Ts};

const PRIMARY_FIELD_NAME: &str = "vstring";

#[derive(Debug)]
pub struct StringSchema;

impl Schema for StringSchema {
    type Record = String;

    type Columns = StringColumns;

    type Key = String;

    fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        static SCHEMA: Lazy<Arc<ArrowSchema>> = Lazy::new(|| {
            Arc::new(ArrowSchema::new(vec![
                Field::new(magic::NULL, DataType::Boolean, false),
                Field::new(magic::TS, DataType::UInt32, false),
                Field::new(PRIMARY_FIELD_NAME, DataType::Utf8, false),
            ]))
        });

        &SCHEMA
    }

    fn primary_key_indices(&self) -> &[usize] {
        const INDICES: [usize; 1] = [2];
        &INDICES
    }

    fn primary_key_paths_and_sorting(&self) -> (&[ColumnPath], &[SortingColumn]) {
        use once_cell::sync::Lazy;
        static PATHS: Lazy<Vec<ColumnPath>> = Lazy::new(|| {
            vec![ColumnPath::new(vec![
                magic::TS.to_string(),
                PRIMARY_FIELD_NAME.to_string(),
            ])]
        });
        static SORTING: Lazy<Vec<SortingColumn>> = Lazy::new(|| {
            vec![
                SortingColumn::new(1, true, true),
                SortingColumn::new(2, false, true),
            ]
        });
        (&PATHS[..], &SORTING[..])
    }
}

impl Record for String {
    type Schema = StringSchema;

    type Ref<'r>
        = &'r str
    where
        Self: 'r;

    fn key(&self) -> &str {
        self
    }

    fn as_record_ref(&self) -> Self::Ref<'_> {
        self
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl<'r> RecordRef<'r> for &'r str {
    type Record = String;

    fn key(self) -> <<<Self::Record as Record>::Schema as Schema>::Key as Key>::Ref<'r> {
        self
    }

    fn projection(&mut self, _: &ProjectionMask) {}

    fn from_record_batch(
        record_batch: &'r RecordBatch,
        offset: usize,
        _: &'r ProjectionMask,
        _: &'r Arc<ArrowSchema>,
    ) -> OptionRecordRef<'r, Self> {
        let ts = record_batch
            .column(1)
            .as_primitive::<UInt32Type>()
            .value(offset)
            .into();
        let vstring = record_batch.column(2).as_string::<i32>().value(offset);
        let null = record_batch.column(0).as_boolean().value(offset);

        OptionRecordRef::new(ts, vstring, null)
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

    fn builder(_schema: Arc<ArrowSchema>, capacity: usize) -> Self::Builder {
        StringColumnsBuilder {
            _null: BooleanBufferBuilder::new(capacity),
            _ts: UInt32Builder::with_capacity(capacity),
            string: StringBuilder::with_capacity(capacity, 0),
        }
    }

    fn get(
        &self,
        offset: u32,
        _: &ProjectionMask,
    ) -> Option<Option<<Self::Record as Record>::Ref<'_>>> {
        if offset as usize >= self.string.len() {
            return None;
        }

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

impl ArrowArraysBuilder<StringColumns> for StringColumnsBuilder {
    fn push(&mut self, key: Ts<&str>, row: Option<&str>) {
        self._null.append(row.is_none());
        self._ts.append_value(key.ts.into());
        if let Some(row) = row {
            self.string.append_value(row);
        } else {
            self.string.append_value(String::default());
        }
    }

    fn written_size(&self) -> usize {
        self._null.as_slice().len()
            + mem::size_of_val(self._ts.values_slice())
            + mem::size_of_val(self.string.values_slice())
    }

    fn finish(&mut self, _: Option<&[usize]>) -> StringColumns {
        let _null = Arc::new(BooleanArray::new(self._null.finish(), None));
        let _ts = Arc::new(self._ts.finish());
        let string = Arc::new(self.string.finish());

        let schema = StringSchema;
        let record_batch = RecordBatch::try_new(
            schema.arrow_schema().clone(),
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

// -----------------------------------------------------------------------------
// Shared test record: Test/TestRef used across crate tests
// -----------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Test {
    pub vstring: String,
    pub vu32: u32,
    pub vbool: Option<bool>,
}

impl fusio_log::Decode for Test {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: fusio::SeqRead,
    {
        let vstring = String::decode(reader).await?;
        let vu32 = Option::<u32>::decode(reader).await?.unwrap();
        let vbool = Option::<bool>::decode(reader).await?;

        Ok(Self {
            vstring,
            vu32,
            vbool,
        })
    }
}

#[cfg(all(test, feature = "tokio"))]
impl Record for Test {
    type Schema = TestSchema;

    type Ref<'r>
        = TestRef<'r>
    where
        Self: 'r;

    fn key(&self) -> &str {
        &self.vstring
    }

    fn as_record_ref(&self) -> Self::Ref<'_> {
        TestRef {
            vstring: &self.vstring,
            vu32: Some(self.vu32),
            vbool: self.vbool,
        }
    }

    fn size(&self) -> usize {
        let string_size = self.vstring.len();
        let u32_size = std::mem::size_of::<u32>();
        let bool_size = self.vbool.map_or(0, |_| std::mem::size_of::<bool>());
        string_size + u32_size + bool_size
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct TestRef<'r> {
    pub vstring: &'r str,
    pub vu32: Option<u32>,
    pub vbool: Option<bool>,
}

impl fusio_log::Encode for TestRef<'_> {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        self.vstring.encode(writer).await?;
        self.vu32.encode(writer).await?;
        self.vbool.encode(writer).await?;
        Ok(())
    }

    fn size(&self) -> usize {
        self.vstring.size() + self.vu32.size() + self.vbool.size()
    }
}

#[cfg(all(test, feature = "tokio"))]
impl<'r> RecordRef<'r> for TestRef<'r> {
    type Record = Test;

    fn key(self) -> <<<Self::Record as Record>::Schema as Schema>::Key as Key>::Ref<'r> {
        self.vstring
    }

    fn projection(&mut self, projection_mask: &ProjectionMask) {
        if !projection_mask.leaf_included(3) {
            self.vu32 = None;
        }
        if !projection_mask.leaf_included(4) {
            self.vbool = None;
        }
    }

    fn from_record_batch(
        record_batch: &'r RecordBatch,
        offset: usize,
        projection_mask: &'r ProjectionMask,
        _: &Arc<ArrowSchema>,
    ) -> OptionRecordRef<'r, Self> {
        let mut column_i = 2;
        let null = record_batch.column(0).as_boolean().value(offset);

        let ts = record_batch
            .column(1)
            .as_primitive::<UInt32Type>()
            .value(offset)
            .into();

        let vstring = record_batch
            .column(column_i)
            .as_string::<i32>()
            .value(offset);
        column_i += 1;

        let mut vu32 = None;
        if projection_mask.leaf_included(3) {
            vu32 = Some(
                record_batch
                    .column(column_i)
                    .as_primitive::<UInt32Type>()
                    .value(offset),
            );
            column_i += 1;
        }

        let mut vbool = None;
        if projection_mask.leaf_included(4) {
            let vbool_array = record_batch.column(column_i).as_boolean();
            if !vbool_array.is_null(offset) {
                vbool = Some(vbool_array.value(offset));
            }
        }

        let record = TestRef {
            vstring,
            vu32,
            vbool,
        };
        OptionRecordRef::new(ts, record, null)
    }
}

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------
#[cfg(test)]
pub(crate) fn test_items<I>(range: I) -> impl Iterator<Item = Test>
where
    I: IntoIterator<Item = u32>,
{
    range.into_iter().map(|i| Test {
        vstring: i.to_string(),
        vu32: i,
        vbool: Some(true),
    })
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) async fn get_test_record_batch<E: crate::executor::Executor + Send + Sync + 'static>(
    option: crate::DbOption,
    executor: E,
) -> arrow::array::RecordBatch {
    use std::{mem, sync::Arc};

    use crate::{executor::RwLock, inmem::mutable::MutableMemTable, DB};

    let db: DB<Test, E> = DB::new(
        option.clone(),
        executor,
        crate::inmem::immutable::tests::TestSchema {},
    )
    .await
    .unwrap();
    let base_fs = db.ctx.manager.base_fs();

    db.write(
        Test {
            vstring: "hello".to_string(),
            vu32: 12,
            vbool: Some(true),
        },
        1.into(),
    )
    .await
    .unwrap();
    db.write(
        Test {
            vstring: "world".to_string(),
            vu32: 12,
            vbool: None,
        },
        1.into(),
    )
    .await
    .unwrap();

    let mut schema = db.mem_storage.write().await;

    let trigger = schema.trigger.clone();
    let mutable = mem::replace(
        &mut schema.mutable,
        MutableMemTable::new(
            &option,
            trigger,
            base_fs.clone(),
            Arc::new(crate::inmem::immutable::tests::TestSchema {}),
        )
        .await
        .unwrap(),
    );

    mutable
        .into_immutable()
        .await
        .unwrap()
        .1
        .as_record_batch()
        .clone()
}
