use std::{collections::BTreeMap, mem::transmute, ops::Bound, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema as ArrowSchema};
use crossbeam_skiplist::SkipMap;
use parquet::arrow::ProjectionMask;

use crate::{
    option::Order,
    record::{
        option::OptionRecordRef, ArrowArrays, ArrowArraysBuilder, Key, Record, RecordRef, Schema,
    },
    stream::record_batch::RecordBatchEntry,
    version::timestamp::{Timestamp, Ts, TsRef, EPOCH},
};

pub(crate) struct ImmutableMemTable<A>
where
    A: ArrowArrays,
{
    data: A,
    index: BTreeMap<Ts<<<A::Record as Record>::Schema as Schema>::Key>, u32>,
}

impl<A> ImmutableMemTable<A>
where
    A: ArrowArrays,
    A::Record: Send,
{
    pub(crate) fn new(
        mutable: SkipMap<Ts<<<A::Record as Record>::Schema as Schema>::Key>, Option<A::Record>>,
        schema: Arc<ArrowSchema>,
    ) -> Self {
        let mut index = BTreeMap::new();
        let mut builder = A::builder(schema, mutable.len());

        for (offset, (key, value)) in mutable.into_iter().enumerate() {
            builder.push(
                Ts::new(key.value.as_key_ref(), key.ts),
                value.as_ref().map(Record::as_record_ref),
            );
            index.insert(key, offset as u32);
        }

        let data = builder.finish(None);

        Self { data, index }
    }
}

impl<A> ImmutableMemTable<A>
where
    A: ArrowArrays,
{
    pub(crate) fn scope(
        &self,
    ) -> (
        Option<&<<A::Record as Record>::Schema as Schema>::Key>,
        Option<&<<A::Record as Record>::Schema as Schema>::Key>,
    ) {
        (
            self.index.first_key_value().map(|(key, _)| key.value()),
            self.index.last_key_value().map(|(key, _)| key.value()),
        )
    }

    pub(crate) fn as_record_batch(&self) -> &RecordBatch {
        self.data.as_record_batch()
    }

    pub(crate) fn scan<'scan>(
        &'scan self,
        range: (
            Bound<&'scan <<A::Record as Record>::Schema as Schema>::Key>,
            Bound<&'scan <<A::Record as Record>::Schema as Schema>::Key>,
        ),
        ts: Timestamp,
        projection_mask: ProjectionMask,
        order: Option<Order>,
    ) -> ImmutableScan<'scan, A::Record> {
        let lower = match range.0 {
            Bound::Included(key) => Bound::Included(TsRef::new(key, ts)),
            Bound::Excluded(key) => Bound::Excluded(TsRef::new(key, EPOCH)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match range.1 {
            Bound::Included(key) => Bound::Included(TsRef::new(key, EPOCH)),
            Bound::Excluded(key) => Bound::Excluded(TsRef::new(key, ts)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let range = self
            .index
            .range::<TsRef<<<A::Record as Record>::Schema as Schema>::Key>, _>((lower, upper));

        let boxed_range: Box<dyn Iterator<Item = _> + Send + 'scan> = if order == Some(Order::Desc)
        {
            Box::new(range.rev())
        } else {
            Box::new(range)
        };

        ImmutableScan::<A::Record>::new(boxed_range, self.data.as_record_batch(), projection_mask)
    }

    pub(crate) fn get(
        &self,
        key: &<<A::Record as Record>::Schema as Schema>::Key,
        ts: Timestamp,
        projection_mask: ProjectionMask,
    ) -> Option<RecordBatchEntry<A::Record>> {
        self.scan(
            (Bound::Included(key), Bound::Included(key)),
            ts,
            projection_mask,
            None, // Order doesn't matter for single-key get
        )
        .next()
    }

    pub(crate) fn check_conflict(
        &self,
        key: &<<A::Record as Record>::Schema as Schema>::Key,
        ts: Timestamp,
    ) -> bool {
        self.index
            .range::<TsRef<<<A::Record as Record>::Schema as Schema>::Key>, _>((
                Bound::Excluded(TsRef::new(key, u32::MAX.into())),
                Bound::Excluded(TsRef::new(key, ts)),
            ))
            .next()
            .is_some()
    }
}

pub(crate) struct ImmutableScan<'iter, R>
where
    R: Record,
{
    range: Box<
        dyn Iterator<Item = (&'iter Ts<<R::Schema as Schema>::Key>, &'iter u32)> + Send + 'iter,
    >,
    record_batch: &'iter RecordBatch,
    projection_mask: ProjectionMask,
}

impl<'iter, R> ImmutableScan<'iter, R>
where
    R: Record,
{
    fn new(
        range: Box<
            dyn Iterator<Item = (&'iter Ts<<R::Schema as Schema>::Key>, &'iter u32)> + Send + 'iter,
        >,
        record_batch: &'iter RecordBatch,
        projection_mask: ProjectionMask,
    ) -> Self {
        Self {
            range,
            record_batch,
            projection_mask,
        }
    }
}

impl<R> Iterator for ImmutableScan<'_, R>
where
    R: Record,
{
    type Item = RecordBatchEntry<R>;

    fn next(&mut self) -> Option<Self::Item> {
        let (_, &offset) = self.range.next()?;

        let schema = self.record_batch.schema();
        let record_ref = R::Ref::from_record_batch(
            self.record_batch,
            offset as usize,
            &self.projection_mask,
            &schema,
        );
        // TODO: remove cloning record batch
        Some(RecordBatchEntry::new(self.record_batch.clone(), {
            // Safety: record_ref self-references the record batch
            unsafe {
                transmute::<OptionRecordRef<R::Ref<'_>>, OptionRecordRef<R::Ref<'static>>>(
                    record_ref,
                )
            }
        }))
    }
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests {
    use std::{mem, sync::Arc};

    use arrow::{
        array::{
            Array, BooleanArray, BooleanBufferBuilder, BooleanBuilder, PrimitiveBuilder,
            RecordBatch, StringArray, StringBuilder, UInt32Array, UInt32Builder,
        },
        datatypes::{ArrowPrimitiveType, DataType, Field, Schema as ArrowSchema, UInt32Type},
    };
    use once_cell::sync::Lazy;
    use parquet::{arrow::ProjectionMask, format::SortingColumn, schema::types::ColumnPath};

    use crate::{
        magic,
        record::{
            test::{Test, TestRef},
            ArrowArrays, ArrowArraysBuilder, Record, Schema,
        },
        version::timestamp::Ts,
    };

    #[derive(Debug)]
    pub struct TestSchema;

    impl Schema for TestSchema {
        type Record = Test;

        type Columns = TestImmutableArrays;

        type Key = String;

        fn arrow_schema(&self) -> &Arc<ArrowSchema> {
            static SCHEMA: Lazy<Arc<ArrowSchema>> = Lazy::new(|| {
                Arc::new(ArrowSchema::new(vec![
                    Field::new("_null", DataType::Boolean, false),
                    Field::new(magic::TS, DataType::UInt32, false),
                    Field::new("vstring", DataType::Utf8, false),
                    Field::new("vu32", DataType::UInt32, false),
                    Field::new("vbool", DataType::Boolean, true),
                ]))
            });

            &SCHEMA
        }

        fn primary_key_index(&self) -> usize {
            2
        }

        fn primary_key_path(
            &self,
        ) -> (
            parquet::schema::types::ColumnPath,
            Vec<parquet::format::SortingColumn>,
        ) {
            (
                ColumnPath::new(vec![magic::TS.to_string(), "vstring".to_string()]),
                vec![
                    SortingColumn::new(1, true, true),
                    SortingColumn::new(2, false, true),
                ],
            )
        }
    }

    #[derive(Debug)]
    pub struct TestImmutableArrays {
        _null: Arc<BooleanArray>,
        _ts: Arc<UInt32Array>,
        vstring: Arc<StringArray>,
        vu32: Arc<UInt32Array>,
        vbool: Arc<BooleanArray>,

        record_batch: RecordBatch,
    }

    impl ArrowArrays for TestImmutableArrays {
        type Record = Test;

        type Builder = TestBuilder;

        fn builder(_schema: Arc<ArrowSchema>, capacity: usize) -> Self::Builder {
            TestBuilder {
                vstring: StringBuilder::with_capacity(capacity, 0),
                vu32: PrimitiveBuilder::<UInt32Type>::with_capacity(capacity),
                vobool: BooleanBuilder::with_capacity(capacity),
                _null: BooleanBufferBuilder::new(capacity),
                _ts: UInt32Builder::with_capacity(capacity),
            }
        }

        fn get(
            &self,
            offset: u32,
            projection_mask: &ProjectionMask,
        ) -> Option<Option<<Self::Record as Record>::Ref<'_>>> {
            let offset = offset as usize;

            if offset >= self.vstring.len() {
                return None;
            }
            if self._null.value(offset) {
                return Some(None);
            }

            let vstring = self.vstring.value(offset);
            let vu32 = projection_mask
                .leaf_included(3)
                .then(|| self.vu32.value(offset));
            let vbool = (!self.vbool.is_null(offset) && projection_mask.leaf_included(4))
                .then(|| self.vbool.value(offset));

            Some(Some(TestRef {
                vstring,
                vu32,
                vbool,
            }))
        }

        fn as_record_batch(&self) -> &RecordBatch {
            &self.record_batch
        }
    }

    pub struct TestBuilder {
        vstring: StringBuilder,
        vu32: PrimitiveBuilder<UInt32Type>,
        vobool: BooleanBuilder,
        _null: BooleanBufferBuilder,
        _ts: UInt32Builder,
    }

    impl ArrowArraysBuilder<TestImmutableArrays> for TestBuilder {
        fn push(&mut self, key: Ts<&str>, row: Option<TestRef>) {
            self.vstring.append_value(key.value);
            match row {
                Some(row) => {
                    self.vu32.append_value(row.vu32.unwrap());
                    match row.vbool {
                        Some(vobool) => self.vobool.append_value(vobool),
                        None => self.vobool.append_null(),
                    }
                    self._null.append(false);
                    self._ts.append_value(key.ts.into());
                }
                None => {
                    self.vu32
                        .append_value(<UInt32Type as ArrowPrimitiveType>::Native::default());
                    self.vobool.append_null();
                    self._null.append(true);
                    self._ts.append_value(key.ts.into());
                }
            }
        }

        fn written_size(&self) -> usize {
            self._null.as_slice().len()
                + mem::size_of_val(self._ts.values_slice())
                + mem::size_of_val(self.vstring.values_slice())
                + mem::size_of_val(self.vu32.values_slice())
                + mem::size_of_val(self.vobool.values_slice())
        }

        fn finish(&mut self, indices: Option<&[usize]>) -> TestImmutableArrays {
            let vstring = Arc::new(self.vstring.finish());
            let vu32 = Arc::new(self.vu32.finish());
            let vbool = Arc::new(self.vobool.finish());
            let _null = Arc::new(BooleanArray::new(self._null.finish(), None));
            let _ts = Arc::new(self._ts.finish());
            let schema = TestSchema;
            let mut record_batch = RecordBatch::try_new(
                Arc::clone(schema.arrow_schema()),
                vec![
                    Arc::clone(&_null) as Arc<dyn Array>,
                    Arc::clone(&_ts) as Arc<dyn Array>,
                    Arc::clone(&vstring) as Arc<dyn Array>,
                    Arc::clone(&vu32) as Arc<dyn Array>,
                    Arc::clone(&vbool) as Arc<dyn Array>,
                ],
            )
            .expect("create record batch must be successful");
            if let Some(indices) = indices {
                record_batch = record_batch
                    .project(indices)
                    .expect("projection indices must be successful");
            }

            TestImmutableArrays {
                vstring,
                vu32,
                vbool,
                _null,
                _ts,
                record_batch,
            }
        }
    }

    #[tokio::test]
    async fn test_immutable_scan_forward() {
        use std::ops::Bound;

        use crossbeam_skiplist::SkipMap;
        use parquet::arrow::ProjectionMask;

        use super::ImmutableMemTable;

        // Create test data with String keys (not &str)
        let skip_map = SkipMap::new();
        skip_map.insert(
            Ts::new("a".to_string(), 1.into()),
            Some(Test {
                vstring: "a".to_string(),
                vu32: 1,
                vbool: Some(true),
            }),
        );
        skip_map.insert(
            Ts::new("b".to_string(), 2.into()),
            Some(Test {
                vstring: "b".to_string(),
                vu32: 2,
                vbool: Some(false),
            }),
        );
        skip_map.insert(
            Ts::new("c".to_string(), 3.into()),
            Some(Test {
                vstring: "c".to_string(),
                vu32: 3,
                vbool: None,
            }),
        );

        let schema = Arc::new(TestSchema);
        let immutable =
            ImmutableMemTable::<TestImmutableArrays>::new(skip_map, schema.arrow_schema().clone());

        // Test forward scan
        let projection = ProjectionMask::all();
        let scan = immutable.scan(
            (Bound::Unbounded, Bound::Unbounded),
            10.into(),
            projection,
            None, // Forward order (default)
        );

        let entries: Vec<_> = scan.collect();
        assert_eq!(entries.len(), 3);

        // Verify forward order: a, b, c
        assert_eq!(entries[0].get().unwrap().vstring, "a");
        assert_eq!(entries[1].get().unwrap().vstring, "b");
        assert_eq!(entries[2].get().unwrap().vstring, "c");
    }

    #[tokio::test]
    async fn test_immutable_scan_reverse() {
        use std::ops::Bound;

        use crossbeam_skiplist::SkipMap;
        use parquet::arrow::ProjectionMask;

        use super::ImmutableMemTable;
        use crate::option::Order;

        // Create test data with String keys (not &str)
        let skip_map = SkipMap::new();
        skip_map.insert(
            Ts::new("a".to_string(), 1.into()),
            Some(Test {
                vstring: "a".to_string(),
                vu32: 1,
                vbool: Some(true),
            }),
        );
        skip_map.insert(
            Ts::new("b".to_string(), 2.into()),
            Some(Test {
                vstring: "b".to_string(),
                vu32: 2,
                vbool: Some(false),
            }),
        );
        skip_map.insert(
            Ts::new("c".to_string(), 3.into()),
            Some(Test {
                vstring: "c".to_string(),
                vu32: 3,
                vbool: None,
            }),
        );

        let schema = Arc::new(TestSchema);
        let immutable =
            ImmutableMemTable::<TestImmutableArrays>::new(skip_map, schema.arrow_schema().clone());

        // Test reverse scan
        let projection = ProjectionMask::all();
        let scan = immutable.scan(
            (Bound::Unbounded, Bound::Unbounded),
            10.into(),
            projection,
            Some(Order::Desc), // Reverse order
        );

        let entries: Vec<_> = scan.collect();
        assert_eq!(entries.len(), 3);

        // Verify reverse order: c, b, a
        assert_eq!(entries[0].get().unwrap().vstring, "c");
        assert_eq!(entries[1].get().unwrap().vstring, "b");
        assert_eq!(entries[2].get().unwrap().vstring, "a");
    }

    #[tokio::test]
    async fn test_immutable_scan_reverse_with_bounds() {
        use std::ops::Bound;

        use crossbeam_skiplist::SkipMap;
        use parquet::arrow::ProjectionMask;

        use super::ImmutableMemTable;
        use crate::option::Order;

        // Create test data with more entries
        let skip_map = SkipMap::new();
        for i in 1..=5 {
            let key = format!("key{}", i);
            skip_map.insert(
                Ts::new(key.clone(), i.into()),
                Some(Test {
                    vstring: key,
                    vu32: i,
                    vbool: Some(i % 2 == 0),
                }),
            );
        }

        let schema = Arc::new(TestSchema);
        let immutable =
            ImmutableMemTable::<TestImmutableArrays>::new(skip_map, schema.arrow_schema().clone());

        // Test reverse scan with bounds: from "key2" to "key4" (inclusive)
        let projection = ProjectionMask::all();
        let key2 = "key2".to_string();
        let key4 = "key4".to_string();
        let scan = immutable.scan(
            (Bound::Included(&key2), Bound::Included(&key4)),
            10.into(),
            projection,
            Some(Order::Desc), // Reverse order
        );

        let entries: Vec<_> = scan.collect();
        assert_eq!(entries.len(), 3);

        // Verify reverse order within bounds: key4, key3, key2
        assert_eq!(entries[0].get().unwrap().vstring, "key4");
        assert_eq!(entries[1].get().unwrap().vstring, "key3");
        assert_eq!(entries[2].get().unwrap().vstring, "key2");
    }
}
