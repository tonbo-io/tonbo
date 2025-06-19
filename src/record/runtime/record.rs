use std::sync::Arc;

use fusio::SeqRead;
use fusio_log::{Decode, Encode};

use super::{schema::DynSchema, DataType, DynRecordRef, Value};
use crate::{
    cast_arc_value,
    record::{Record, RecordDecodeError, Timestamp, F32, F64},
};

#[derive(Debug)]
pub struct DynRecord {
    values: Vec<Value>,
    primary_index: usize,
}

#[allow(unused)]
impl DynRecord {
    pub fn new(values: Vec<Value>, primary_index: usize) -> Self {
        Self {
            values,
            primary_index,
        }
    }
}

impl Decode for DynRecord {
    type Error = RecordDecodeError;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: SeqRead,
    {
        let len = u32::decode(reader).await? as usize;
        let primary_index = u32::decode(reader).await? as usize;
        let mut values = vec![];
        // keep invariant for record: nullable --> Some(v); non-nullable --> v
        for i in 0..len {
            let mut col = Value::decode(reader).await?;
            if i != primary_index && !col.is_nullable() {
                col.value = match col.datatype() {
                    DataType::UInt8 => Arc::new(cast_arc_value!(col.value, Option<u8>).unwrap()),
                    DataType::UInt16 => Arc::new(cast_arc_value!(col.value, Option<u16>).unwrap()),
                    DataType::UInt32 => Arc::new(cast_arc_value!(col.value, Option<u32>).unwrap()),
                    DataType::UInt64 => Arc::new(cast_arc_value!(col.value, Option<u64>).unwrap()),
                    DataType::Int8 => Arc::new(cast_arc_value!(col.value, Option<i8>).unwrap()),
                    DataType::Int16 => Arc::new(cast_arc_value!(col.value, Option<i16>).unwrap()),
                    DataType::Int32 => Arc::new(cast_arc_value!(col.value, Option<i32>).unwrap()),
                    DataType::Int64 => Arc::new(cast_arc_value!(col.value, Option<i64>).unwrap()),
                    DataType::Float32 => Arc::new(cast_arc_value!(col.value, Option<F32>).unwrap()),
                    DataType::Float64 => Arc::new(cast_arc_value!(col.value, Option<F64>).unwrap()),
                    DataType::String => {
                        Arc::new(cast_arc_value!(col.value, Option<String>).clone().unwrap())
                    }
                    DataType::Boolean => {
                        Arc::new(cast_arc_value!(col.value, Option<bool>).unwrap())
                    }
                    DataType::Bytes => {
                        Arc::new(cast_arc_value!(col.value, Option<Vec<u8>>).clone().unwrap())
                    }
                    DataType::Timestamp(_) => {
                        Arc::new(cast_arc_value!(col.value, Option<Timestamp>).unwrap())
                    }
                };
            }
            values.push(col);
        }

        Ok(DynRecord {
            values,
            primary_index,
        })
    }
}

impl Record for DynRecord {
    type Schema = DynSchema;

    type Ref<'r> = DynRecordRef<'r>;

    fn as_record_ref(&self) -> Self::Ref<'_> {
        let mut columns = vec![];
        for (idx, col) in self.values.iter().enumerate() {
            let datatype = col.datatype();
            let is_nullable = col.is_nullable();
            let mut value = col.value.clone();
            if idx != self.primary_index && !is_nullable {
                value = match datatype {
                    DataType::UInt8 => Arc::new(Some(*cast_arc_value!(col.value, u8))),
                    DataType::UInt16 => Arc::new(Some(*cast_arc_value!(col.value, u16))),
                    DataType::UInt32 => Arc::new(Some(*cast_arc_value!(col.value, u32))),
                    DataType::UInt64 => Arc::new(Some(*cast_arc_value!(col.value, u64))),
                    DataType::Int8 => Arc::new(Some(*cast_arc_value!(col.value, i8))),
                    DataType::Int16 => Arc::new(Some(*cast_arc_value!(col.value, i16))),
                    DataType::Int32 => Arc::new(Some(*cast_arc_value!(col.value, i32))),
                    DataType::Int64 => Arc::new(Some(*cast_arc_value!(col.value, i64))),
                    DataType::Float32 => Arc::new(Some(*cast_arc_value!(col.value, F32))),
                    DataType::Float64 => Arc::new(Some(*cast_arc_value!(col.value, F64))),
                    DataType::String => {
                        Arc::new(Some(cast_arc_value!(col.value, String).to_owned()))
                    }
                    DataType::Boolean => Arc::new(Some(*cast_arc_value!(col.value, bool))),
                    DataType::Bytes => {
                        Arc::new(Some(cast_arc_value!(col.value, Vec<u8>).to_owned()))
                    }
                    DataType::Timestamp(_) => {
                        Arc::new(Some(*cast_arc_value!(col.value, Timestamp)))
                    }
                };
            }

            columns.push(Value::new(
                datatype,
                col.desc.name.to_owned(),
                value,
                is_nullable,
            ));
        }
        DynRecordRef::new(columns, self.primary_index)
    }

    fn size(&self) -> usize {
        self.values.iter().fold(0, |acc, col| acc + col.size())
    }
}

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
/// use tonbo::dyn_record;
///
/// let record = dyn_record!(
///     ("foo", String, false, "hello".to_owned()),
///     ("bar", Int32, true, 1_i32),
///     ("baz", UInt64, true, 1_u64),
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
                        $crate::record::Value::new(
                            $crate::record::DataType::$type,
                            $name.into(),
                            std::sync::Arc::new($value),
                            $nullable,
                        ),
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
                        $crate::record::Value::new(
                            $type,
                            $name.into(),
                            std::sync::Arc::new($value),
                            $nullable,
                        ),
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

    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use super::{DynRecord, DynSchema, Record};
    use crate::{
        make_dyn_schema,
        record::{DataType, DynRecordRef, TimeUnit, Timestamp, Value, F32, F64},
    };

    #[allow(unused)]
    pub(crate) fn test_dyn_item_schema() -> DynSchema {
        make_dyn_schema!(
            ("id", DataType::Int64, false),
            ("age", DataType::Int8, true),
            ("height", DataType::Int16, true),
            ("weight", DataType::Int32, false),
            ("name", DataType::String, false),
            ("email", DataType::String, true),
            ("enabled", DataType::Boolean, false),
            ("bytes", DataType::Bytes, true),
            ("grade", DataType::Float32, false),
            ("price", DataType::Float64, true),
            (
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond),
                true
            ),
            0
        )
    }

    #[allow(unused)]
    pub(crate) fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let mut record = make_dyn_record!(
                ("id", DataType::Int64, false, i as i64),
                ("age", DataType::Int8, true, Some(i as i8)),
                ("height", DataType::Int16, true, Some(i as i16 * 20)),
                ("weight", DataType::Int32, false, i * 200_i32),
                ("name", DataType::String, false, i.to_string()),
                (
                    "email",
                    DataType::String,
                    true,
                    Some(format!("{}@tonbo.io", i))
                ),
                ("enabled", DataType::Boolean, false, i % 2 == 0),
                (
                    "bytes",
                    DataType::Bytes,
                    true,
                    Some(i.to_le_bytes().to_vec())
                ),
                (
                    "grade",
                    DataType::Float32,
                    false,
                    F32::from(i as f32 * 1.11)
                ),
                (
                    "price",
                    DataType::Float64,
                    true,
                    Some(F64::from(i as f64 * 1.01))
                ),
                (
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond),
                    true,
                    Some(Timestamp::new_millis(i as i64))
                ),
                0
            );
            if i >= 45 {
                record.values[2].value = Arc::<Option<i16>>::new(None);
            }

            items.push(record);
        }
        items
    }

    fn test_dyn_record() -> DynRecord {
        make_dyn_record!(
            ("id", DataType::Int64, false, 10i64),
            ("age", DataType::Int8, true, Some(10i8)),
            ("height", DataType::Int16, true, Some(183i16)),
            ("weight", DataType::Int32, false, 56i32),
            ("name", DataType::String, false, "tonbo".to_string()),
            (
                "email",
                DataType::String,
                true,
                Some("contact@tonbo.io".to_string())
            ),
            ("enabled", DataType::Boolean, false, true),
            (
                "bytes",
                DataType::Bytes,
                true,
                Some(b"hello tonbo".to_vec())
            ),
            ("grade", DataType::Float32, false, F32::from(1.1234)),
            ("price", DataType::Float64, true, Some(F64::from(1.01))),
            (
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond),
                true,
                Some(Timestamp::new_millis(1717507203412))
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
                Value::new(DataType::Int64, "id".to_string(), Arc::new(10i64), false),
                Value::new(
                    DataType::Int8,
                    "age".to_string(),
                    Arc::new(Some(10i8)),
                    true,
                ),
                Value::new(
                    DataType::Int16,
                    "height".to_string(),
                    Arc::new(Some(183i16)),
                    true,
                ),
                Value::new(
                    DataType::Int32,
                    "weight".to_string(),
                    Arc::new(Some(56i32)),
                    false,
                ),
                Value::new(
                    DataType::String,
                    "name".to_string(),
                    Arc::new(Some("tonbo".to_string())),
                    false,
                ),
                Value::new(
                    DataType::String,
                    "email".to_string(),
                    Arc::new(Some("contact@tonbo.io".to_string())),
                    true,
                ),
                Value::new(
                    DataType::Boolean,
                    "enabled".to_string(),
                    Arc::new(Some(true)),
                    false,
                ),
                Value::new(
                    DataType::Bytes,
                    "bytes".to_string(),
                    Arc::new(Some(b"hello tonbo".to_vec())),
                    true,
                ),
                Value::new(
                    DataType::Float32,
                    "grade".to_string(),
                    Arc::new(Some(F32::from(1.1234))),
                    false,
                ),
                Value::new(
                    DataType::Float64,
                    "price".to_string(),
                    Arc::new(Some(F64::from(1.01))),
                    true,
                ),
                Value::new(
                    DataType::Timestamp(TimeUnit::Millisecond),
                    "timestamp".to_string(),
                    Arc::new(Some(Timestamp::new_millis(1717507203412))),
                    true,
                ),
            ],
            0,
        );

        for (actual, expected) in record_ref.columns.iter().zip(expected.columns) {
            assert_eq!(*actual, expected)
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
