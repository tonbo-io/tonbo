use std::{mem, sync::Arc};

use arrow::{
    array::{
        Array, AsArray, BooleanArray, BooleanBufferBuilder, Datum, RecordBatch, StringArray,
        StringBuilder, UInt32Array, UInt32Array as ArrowUInt32Array, UInt32Builder,
    },
    datatypes::{DataType, Field, Schema as ArrowSchema, UInt32Type},
};
use once_cell::sync::Lazy;
use parquet::{arrow::ProjectionMask, format::SortingColumn, schema::types::ColumnPath};

use super::{option::OptionRecordRef, Decode, Encode, Key, KeyRef, Record, RecordRef, Schema};
use crate::{
    inmem::immutable::{ArrowArrays, Builder},
    magic,
    version::timestamp::Ts,
};

// Composite key with (user_id: String, post_id: u32)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompositeKey {
    pub user_id: String,
    pub post_id: u32,
}

impl Encode for CompositeKey {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        self.user_id.encode(writer).await?;
        self.post_id.encode(writer).await
    }

    fn size(&self) -> usize {
        self.user_id.size() + self.post_id.size()
    }
}

impl Decode for CompositeKey {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: fusio::SeqRead,
    {
        let user_id = String::decode(reader).await?;
        let post_id = u32::decode(reader).await?;
        Ok(CompositeKey { user_id, post_id })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompositeKeyRef<'r> {
    pub user_id: &'r str,
    pub post_id: u32,
}

impl Encode for CompositeKeyRef<'_> {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        self.user_id.encode(writer).await?;
        self.post_id.encode(writer).await
    }

    fn size(&self) -> usize {
        self.user_id.size() + self.post_id.size()
    }
}

impl<'r> KeyRef<'r> for CompositeKeyRef<'r> {
    type Key = CompositeKey;

    fn to_key(self) -> Self::Key {
        CompositeKey {
            user_id: self.user_id.to_string(),
            post_id: self.post_id,
        }
    }
}

impl Key for CompositeKey {
    type Ref<'r>
        = CompositeKeyRef<'r>
    where
        Self: 'r;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        CompositeKeyRef {
            user_id: &self.user_id,
            post_id: self.post_id,
        }
    }

    fn to_arrow_fields(&self) -> Vec<Arc<dyn Datum>> {
        vec![
            Arc::new(StringArray::new_scalar(&self.user_id)),
            Arc::new(ArrowUInt32Array::new_scalar(self.post_id)),
        ]
    }
}

// Test record with composite key
#[derive(Debug, Clone, PartialEq)]
pub struct UserPost {
    pub user_id: String,
    pub post_id: u32,
    pub title: String,
    pub content: String,
}

impl Encode for UserPost {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        self.user_id.encode(writer).await?;
        self.post_id.encode(writer).await?;
        self.title.encode(writer).await?;
        self.content.encode(writer).await
    }

    fn size(&self) -> usize {
        self.user_id.size() + self.post_id.size() + self.title.size() + self.content.size()
    }
}

impl Decode for UserPost {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: fusio::SeqRead,
    {
        let user_id = String::decode(reader).await?;
        let post_id = u32::decode(reader).await?;
        let title = String::decode(reader).await?;
        let content = String::decode(reader).await?;
        Ok(UserPost {
            user_id,
            post_id,
            title,
            content,
        })
    }
}

impl Record for UserPost {
    type Schema = UserPostSchema;

    type Ref<'r>
        = UserPostRef<'r>
    where
        Self: 'r;

    fn key(&self) -> CompositeKeyRef<'_> {
        CompositeKeyRef {
            user_id: &self.user_id,
            post_id: self.post_id,
        }
    }

    fn as_record_ref(&self) -> Self::Ref<'_> {
        UserPostRef {
            user_id: &self.user_id,
            post_id: self.post_id,
            title: &self.title,
            content: &self.content,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UserPostRef<'r> {
    pub user_id: &'r str,
    pub post_id: u32,
    pub title: &'r str,
    pub content: &'r str,
}

impl Encode for UserPostRef<'_> {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        self.user_id.encode(writer).await?;
        self.post_id.encode(writer).await?;
        self.title.encode(writer).await?;
        self.content.encode(writer).await
    }

    fn size(&self) -> usize {
        self.user_id.size() + self.post_id.size() + self.title.size() + self.content.size()
    }
}

impl<'r> RecordRef<'r> for UserPostRef<'r> {
    type Record = UserPost;

    fn key(self) -> CompositeKeyRef<'r> {
        CompositeKeyRef {
            user_id: self.user_id,
            post_id: self.post_id,
        }
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
        let user_id = record_batch.column(2).as_string::<i32>().value(offset);
        let post_id = record_batch
            .column(3)
            .as_primitive::<UInt32Type>()
            .value(offset);
        let title = record_batch.column(4).as_string::<i32>().value(offset);
        let content = record_batch.column(5).as_string::<i32>().value(offset);
        let null = record_batch.column(0).as_boolean().value(offset);

        OptionRecordRef::new(
            ts,
            UserPostRef {
                user_id,
                post_id,
                title,
                content,
            },
            null,
        )
    }
}

#[derive(Debug)]
pub struct UserPostSchema;

impl Schema for UserPostSchema {
    type Record = UserPost;
    type Columns = UserPostColumns;
    type Key = CompositeKey;

    fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        static SCHEMA: Lazy<Arc<ArrowSchema>> = Lazy::new(|| {
            Arc::new(ArrowSchema::new(vec![
                Field::new("_null", DataType::Boolean, false),
                Field::new(magic::TS, DataType::UInt32, false),
                Field::new("user_id", DataType::Utf8, false),
                Field::new("post_id", DataType::UInt32, false),
                Field::new("title", DataType::Utf8, false),
                Field::new("content", DataType::Utf8, false),
            ]))
        });

        &SCHEMA
    }

    fn primary_key_indices(&self) -> &[usize] {
        &[2, 3] // user_id and post_id
    }

    fn primary_key_paths(&self) -> Vec<(ColumnPath, Vec<SortingColumn>)> {
        vec![
            (
                ColumnPath::new(vec![magic::TS.to_string(), "user_id".to_string()]),
                vec![
                    SortingColumn::new(1, true, true),
                    SortingColumn::new(2, false, true),
                ],
            ),
            (
                ColumnPath::new(vec![magic::TS.to_string(), "post_id".to_string()]),
                vec![
                    SortingColumn::new(1, true, true),
                    SortingColumn::new(3, false, true),
                ],
            ),
        ]
    }
}

pub struct UserPostColumns {
    _null: Arc<BooleanArray>,
    _ts: Arc<UInt32Array>,
    user_id: Arc<StringArray>,
    post_id: Arc<UInt32Array>,
    title: Arc<StringArray>,
    content: Arc<StringArray>,

    record_batch: RecordBatch,
}

impl ArrowArrays for UserPostColumns {
    type Record = UserPost;
    type Builder = UserPostColumnsBuilder;

    fn builder(_schema: Arc<ArrowSchema>, capacity: usize) -> Self::Builder {
        UserPostColumnsBuilder {
            _null: BooleanBufferBuilder::new(capacity),
            _ts: UInt32Builder::with_capacity(capacity),
            user_id: StringBuilder::with_capacity(capacity, 0),
            post_id: UInt32Builder::with_capacity(capacity),
            title: StringBuilder::with_capacity(capacity, 0),
            content: StringBuilder::with_capacity(capacity, 0),
        }
    }

    fn get(&self, offset: u32, _: &ProjectionMask) -> Option<Option<UserPostRef<'_>>> {
        if offset as usize >= self.user_id.len() {
            return None;
        }

        if self._null.value(offset as usize) {
            return Some(None);
        }

        Some(Some(UserPostRef {
            user_id: self.user_id.value(offset as usize),
            post_id: self.post_id.value(offset as usize),
            title: self.title.value(offset as usize),
            content: self.content.value(offset as usize),
        }))
    }

    fn as_record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }
}

pub struct UserPostColumnsBuilder {
    _null: BooleanBufferBuilder,
    _ts: UInt32Builder,
    user_id: StringBuilder,
    post_id: UInt32Builder,
    title: StringBuilder,
    content: StringBuilder,
}

impl Builder<UserPostColumns> for UserPostColumnsBuilder {
    fn push(&mut self, key: Ts<CompositeKeyRef<'_>>, row: Option<UserPostRef<'_>>) {
        self._null.append(row.is_none());
        self._ts.append_value(key.ts.into());

        if let Some(row) = row {
            self.user_id.append_value(row.user_id);
            self.post_id.append_value(row.post_id);
            self.title.append_value(row.title);
            self.content.append_value(row.content);
        } else {
            self.user_id.append_value("");
            self.post_id.append_value(0);
            self.title.append_value("");
            self.content.append_value("");
        }
    }

    fn written_size(&self) -> usize {
        self._null.as_slice().len()
            + mem::size_of_val(self._ts.values_slice())
            + mem::size_of_val(self.user_id.values_slice())
            + mem::size_of_val(self.post_id.values_slice())
            + mem::size_of_val(self.title.values_slice())
            + mem::size_of_val(self.content.values_slice())
    }

    fn finish(&mut self, _: Option<&[usize]>) -> UserPostColumns {
        let _null = Arc::new(BooleanArray::new(self._null.finish(), None));
        let _ts = Arc::new(self._ts.finish());
        let user_id = Arc::new(self.user_id.finish());
        let post_id = Arc::new(self.post_id.finish());
        let title = Arc::new(self.title.finish());
        let content = Arc::new(self.content.finish());

        let schema = UserPostSchema;
        let record_batch = RecordBatch::try_new(
            schema.arrow_schema().clone(),
            vec![
                Arc::clone(&_null) as Arc<dyn Array>,
                Arc::clone(&_ts) as Arc<dyn Array>,
                Arc::clone(&user_id) as Arc<dyn Array>,
                Arc::clone(&post_id) as Arc<dyn Array>,
                Arc::clone(&title) as Arc<dyn Array>,
                Arc::clone(&content) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        UserPostColumns {
            _null,
            _ts,
            user_id,
            post_id,
            title,
            content,
            record_batch,
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{ops::Bound, sync::Arc};

    use fusio::{disk::TokioFs, path::Path, DynFs};

    use super::*;
    use crate::{
        inmem::mutable::MutableMemTable, trigger::TriggerFactory, wal::log::LogType, DbOption,
    };

    #[tokio::test]
    async fn test_composite_key_ordering() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let schema = Arc::new(UserPostSchema);
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &*schema,
        );
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);
        let mem_table =
            MutableMemTable::<UserPost>::new(&option, trigger, fs.clone(), schema.clone())
                .await
                .unwrap();

        // Insert records with composite keys
        // These should be ordered by (user_id, post_id)
        let records = vec![
            UserPost {
                user_id: "alice".to_string(),
                post_id: 2,
                title: "Alice's second post".to_string(),
                content: "Content 2".to_string(),
            },
            UserPost {
                user_id: "bob".to_string(),
                post_id: 1,
                title: "Bob's first post".to_string(),
                content: "Content 3".to_string(),
            },
            UserPost {
                user_id: "alice".to_string(),
                post_id: 1,
                title: "Alice's first post".to_string(),
                content: "Content 1".to_string(),
            },
            UserPost {
                user_id: "bob".to_string(),
                post_id: 3,
                title: "Bob's third post".to_string(),
                content: "Content 5".to_string(),
            },
            UserPost {
                user_id: "alice".to_string(),
                post_id: 3,
                title: "Alice's third post".to_string(),
                content: "Content 4".to_string(),
            },
        ];

        // Insert all records at timestamp 0
        for record in records {
            mem_table
                .insert(LogType::Full, record, 0_u32.into())
                .await
                .unwrap();
        }

        // Scan all records and verify ordering
        let mut scan = mem_table.scan((Bound::Unbounded, Bound::Unbounded), 0_u32.into());

        // Expected order:
        // 1. ("alice", 1)
        // 2. ("alice", 2)
        // 3. ("alice", 3)
        // 4. ("bob", 1)
        // 5. ("bob", 3)

        let entry = scan.next().unwrap();
        let record = entry.value().as_ref().unwrap();
        assert_eq!(record.user_id, "alice");
        assert_eq!(record.post_id, 1);

        let entry = scan.next().unwrap();
        let record = entry.value().as_ref().unwrap();
        assert_eq!(record.user_id, "alice");
        assert_eq!(record.post_id, 2);

        let entry = scan.next().unwrap();
        let record = entry.value().as_ref().unwrap();
        assert_eq!(record.user_id, "alice");
        assert_eq!(record.post_id, 3);

        let entry = scan.next().unwrap();
        let record = entry.value().as_ref().unwrap();
        assert_eq!(record.user_id, "bob");
        assert_eq!(record.post_id, 1);

        let entry = scan.next().unwrap();
        let record = entry.value().as_ref().unwrap();
        assert_eq!(record.user_id, "bob");
        assert_eq!(record.post_id, 3);

        assert!(scan.next().is_none());

        // Test get with composite key
        let key = CompositeKey {
            user_id: "alice".to_string(),
            post_id: 2,
        };
        let entry = mem_table.get(&key, 0_u32.into()).unwrap();
        let record = entry.value().as_ref().unwrap();
        assert_eq!(record.title, "Alice's second post");

        // Test range scan with composite keys
        let lower_key = CompositeKey {
            user_id: "alice".to_string(),
            post_id: 2,
        };
        let upper_key = CompositeKey {
            user_id: "bob".to_string(),
            post_id: 2,
        };

        let mut scan = mem_table.scan(
            (Bound::Included(&lower_key), Bound::Excluded(&upper_key)),
            0_u32.into(),
        );

        // Should get: ("alice", 2), ("alice", 3), ("bob", 1)
        let entry = scan.next().unwrap();
        let record = entry.value().as_ref().unwrap();
        assert_eq!(record.user_id, "alice");
        assert_eq!(record.post_id, 2);

        let entry = scan.next().unwrap();
        let record = entry.value().as_ref().unwrap();
        assert_eq!(record.user_id, "alice");
        assert_eq!(record.post_id, 3);

        let entry = scan.next().unwrap();
        let record = entry.value().as_ref().unwrap();
        assert_eq!(record.user_id, "bob");
        assert_eq!(record.post_id, 1);

        assert!(scan.next().is_none());
    }

    #[tokio::test]
    async fn test_composite_key_conflict_detection() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let schema = Arc::new(UserPostSchema);
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &*schema,
        );
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);
        let mem_table =
            MutableMemTable::<UserPost>::new(&option, trigger, fs.clone(), schema.clone())
                .await
                .unwrap();

        // Insert a record at timestamp 5
        let record = UserPost {
            user_id: "alice".to_string(),
            post_id: 1,
            title: "Title".to_string(),
            content: "Content".to_string(),
        };
        mem_table
            .insert(LogType::Full, record, 5_u32.into())
            .await
            .unwrap();

        // Check conflict detection
        let key = CompositeKey {
            user_id: "alice".to_string(),
            post_id: 1,
        };

        // Should detect conflict for earlier timestamp
        assert!(mem_table.check_conflict(&key, 3_u32.into()));

        // Should not detect conflict for later timestamp
        assert!(!mem_table.check_conflict(&key, 10_u32.into()));

        // Should not detect conflict for different key
        let different_key = CompositeKey {
            user_id: "alice".to_string(),
            post_id: 2,
        };
        assert!(!mem_table.check_conflict(&different_key, 3_u32.into()));
    }
}
