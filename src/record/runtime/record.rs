use std::sync::Arc;

use common::{
    datatype::DataType, util::decode_value, Date32, Date64, LargeBinary, LargeString, PrimaryKey,
    Time32, Time64, Timestamp, Value, F32, F64,
};
use fusio::SeqRead;
use fusio_log::{Decode, Encode};

use super::{DynRecordImmutableArrays, DynRecordRef};
use crate::record::{PrimaryKeyRef, Record};

#[derive(Debug)]
pub struct DynRecord {
    values: Vec<Arc<dyn Value>>,
    primary_index: usize,
}

#[allow(unused)]
impl DynRecord {
    pub fn new(values: Vec<Arc<dyn Value>>, primary_index: usize) -> Self {
        Self {
            values,
            primary_index,
        }
    }
}

macro_rules! implement_record {
    (
        { $( { $copy_ty:ty, $copy_pat:pat}), * $(,)? },
        { $( { $clone_ty:ty, $clone_pat:pat}), * $(,)? },
    ) => {
        impl Decode for DynRecord {

            async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
            where
                R: SeqRead,
            {
                let len = u32::decode(reader).await? as usize;
                let primary_index = u32::decode(reader).await? as usize;
                let mut values = Vec::with_capacity(len);
                // keep invariant for record: nullable --> Some(v); non-nullable --> v
                for i in 0..len {
                    let is_nullable = bool::decode(reader).await?;
                    let col = decode_value(reader).await?;
                    if is_nullable || i == primary_index {
                        values.push(col);
                    } else {
                        let val: Arc<dyn Value> = match col.data_type() {
                            $(
                                $copy_pat => {
                                    Arc::new(col.as_any().downcast_ref::<Option<$copy_ty>>().unwrap().unwrap())
                                }
                            )*
                            $(
                                $clone_pat => {
                                    Arc::new(col.as_any().downcast_ref::<Option<$clone_ty>>().unwrap().clone().unwrap())
                                }
                            )*
                        };
                        values.push(val);
                    }

                }

                Ok(DynRecord {
                    values,
                    primary_index,
                })
            }
        }

        impl Record for DynRecord {
            // type Key = PrimaryKey;

            type Ref<'r> = DynRecordRef<'r>;

            type Columns = DynRecordImmutableArrays;

            fn key(&self) -> PrimaryKeyRef<'_> {
                let idx = self.primary_index;
                PrimaryKeyRef {
                    keys: vec![self.values[idx].as_ref()],
                }
            }

            fn as_record_ref(&self) -> Self::Ref<'_> {
                let mut columns = vec![];
                for (idx, col) in self.values.iter().enumerate() {
                    let datatype = col.data_type();
                    let is_nullable = col.is_some() || col.is_none();
                    let mut value = col.clone();
                    if idx != self.primary_index && !is_nullable {
                        value = match datatype {
                            $(
                                $copy_pat => Arc::new(Some(*col.as_any().downcast_ref::<$copy_ty>().unwrap())),
                            )*
                            $(
                                $clone_pat => Arc::new(Some(col.as_any().downcast_ref::<$clone_ty>().unwrap().clone())),
                            )*
                        };
                    }

                    columns.push(value,);
                }
                DynRecordRef::new(columns, self.primary_index)
            }

            fn size(&self) -> usize {
                self.values.iter().fold(0, |acc, col| acc + col.size())
            }
        }
    };
}

implement_record!(
    {
        // types that can be copied
        { u8, DataType::UInt8 },
        { u16, DataType::UInt16 },
        { u32, DataType::UInt32 },
        { u64, DataType::UInt64 },
        { i8, DataType::Int8 },
        { i16, DataType::Int16 },
        { i32, DataType::Int32 },
        { i64, DataType::Int64 },
        { F32, DataType::Float32 },
        { F64, DataType::Float64 },
        { bool, DataType::Boolean },
        { Timestamp, DataType::Timestamp(_) },
        { Time32, DataType::Time32(_) },
        { Time64, DataType::Time64(_) },
        { Date32, DataType::Date32 },
        { Date64, DataType::Date64 }
    },
    {
        // types that can be cloned
        { Vec<u8>, DataType::Bytes },
        { LargeBinary, DataType::LargeBinary },
        { String, DataType::String },
        { LargeString, DataType::LargeString }
    },
);

/// Creates a [`DynRecord`] from slice of values and primary key index, suitable for rapid
/// testing and development.
///
/// ## Example:
///
/// ```no_run
/// // dyn_record!(
/// //      (name, type, nullable, value),
/// //         ......
/// //      (name, type, nullable, value),
/// //      primary_key_index
/// // );
/// use std::sync::Arc;
///
/// use tonbo::dyn_record;
///
/// let record = dyn_record!(
///     ("foo", String, false, Arc::new("hello".to_owned())),
///     ("bar", Int32, true, Arc::new(1_i32)),
///     ("baz", UInt64, true, Arc::new(1_u64)),
///     0
/// );
/// ```
#[macro_export]
macro_rules! dyn_record {
    ($(($name: expr, $type: ident, $nullable: expr, $value: expr)),*, $primary: literal) => {
        {
            $crate::record::DynRecord::new(
                vec![
                    $(
                        $value,
                    )*
                ],
                $primary,
            )
        }
    }
}

#[macro_export]
macro_rules! make_dyn_record {
    ($(($name: expr, $type: expr, $nullable: expr, $value: expr)),*, $primary: literal) => {
        {
            $crate::record::DynRecord::new(
                vec![
                    $(
                        $value,
                    )*
                ],
                $primary,
            )
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{
        io::{Cursor, SeekFrom},
        sync::Arc,
    };

    use arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
    use common::{datatype::DataType, TimeUnit, Timestamp, F32, F64};
    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use super::{DynRecord, Record};
    use crate::record::{DynRecordRef, Schema};

    #[allow(unused)]
    pub(crate) fn test_dyn_item_schema() -> Schema {
        Schema::new(
            vec![
                Field::new("id", ArrowDataType::Int64, false),
                Field::new("age", ArrowDataType::Int8, true),
                Field::new("height", ArrowDataType::Int16, true),
                Field::new("weight", ArrowDataType::Int32, false),
                Field::new("name", ArrowDataType::Utf8, false),
                Field::new("email", ArrowDataType::Utf8, true),
                Field::new("enabled", ArrowDataType::Boolean, false),
                Field::new("bytes", ArrowDataType::Binary, true),
                Field::new("grade", ArrowDataType::Float32, false),
                Field::new("price", ArrowDataType::Float64, true),
                Field::new(
                    "timestamp",
                    ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                    true,
                ),
            ],
            0,
        )
    }

    #[allow(unused)]
    pub(crate) fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let mut record = make_dyn_record!(
                ("id", DataType::Int64, false, Arc::new(i as i64)),
                ("age", DataType::Int8, true, Arc::new(Some(i as i8))),
                (
                    "height",
                    DataType::Int16,
                    true,
                    Arc::new(Some(i as i16 * 20))
                ),
                ("weight", DataType::Int32, false, Arc::new(i * 200_i32)),
                ("name", DataType::String, false, Arc::new(i.to_string())),
                (
                    "email",
                    DataType::String,
                    true,
                    Arc::new(Some(format!("{}@tonbo.io", i)))
                ),
                ("enabled", DataType::Boolean, false, Arc::new(i % 2 == 0)),
                (
                    "bytes",
                    DataType::Bytes,
                    true,
                    Arc::new(Some(i.to_le_bytes().to_vec()))
                ),
                (
                    "grade",
                    DataType::Float32,
                    false,
                    Arc::new(F32::from(i as f32 * 1.11))
                ),
                (
                    "price",
                    DataType::Float64,
                    true,
                    Arc::new(Some(F64::from(i as f64 * 1.01)))
                ),
                (
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond),
                    true,
                    Arc::new(Some(Timestamp::new_millis(i as i64)))
                ),
                0
            );
            if i >= 45 {
                record.values[2] = Arc::<Option<i16>>::new(None);
            }

            items.push(record);
        }
        items
    }

    fn test_dyn_record() -> DynRecord {
        make_dyn_record!(
            ("id", DataType::Int64, false, Arc::new(10i64)),
            ("age", DataType::Int8, true, Arc::new(Some(10i8))),
            ("height", DataType::Int16, true, Arc::new(Some(183i16))),
            ("weight", DataType::Int32, false, Arc::new(56i32)),
            (
                "name",
                DataType::String,
                false,
                Arc::new("tonbo".to_string())
            ),
            (
                "email",
                DataType::String,
                true,
                Arc::new(Some("contact@tonbo.io".to_string()))
            ),
            ("enabled", DataType::Boolean, false, Arc::new(true)),
            (
                "bytes",
                DataType::Bytes,
                true,
                Arc::new(Some(b"hello tonbo".to_vec()))
            ),
            (
                "grade",
                DataType::Float32,
                false,
                Arc::new(F32::from(1.1234))
            ),
            (
                "price",
                DataType::Float64,
                true,
                Arc::new(Some(F64::from(1.01)))
            ),
            (
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond),
                true,
                Arc::new(Some(Timestamp::new_millis(1717507203412)))
            ),
            0
        )
    }

    #[test]
    fn test_as_record_ref() {
        let record = test_dyn_record();
        let record_ref = record.as_record_ref();
        let expected = DynRecordRef::new(
            vec![
                Arc::new(10i64),
                Arc::new(Some(10i8)),
                Arc::new(Some(183i16)),
                Arc::new(Some(56i32)),
                Arc::new(Some("tonbo".to_string())),
                Arc::new(Some("contact@tonbo.io".to_string())),
                Arc::new(Some(true)),
                Arc::new(Some(b"hello tonbo".to_vec())),
                Arc::new(Some(F32::from(1.1234))),
                Arc::new(Some(F64::from(1.01))),
                Arc::new(Some(Timestamp::new_millis(1717507203412))),
            ],
            0,
        );

        for (actual, expected) in record_ref.columns.iter().zip(expected.columns) {
            assert_eq!(actual, &expected)
        }
    }

    #[tokio::test]
    async fn test_encode_decode_dyn_record() {
        let record = test_dyn_record();

        let mut bytes = Vec::new();
        let mut buf = Cursor::new(&mut bytes);
        let record_ref = record.as_record_ref();
        record_ref.encode(&mut buf).await.unwrap();

        buf.seek(SeekFrom::Start(0)).await.unwrap();
        let actual = DynRecord::decode(&mut buf).await.unwrap();

        assert_eq!(
            record.as_record_ref().columns,
            actual.as_record_ref().columns
        );
    }
}
