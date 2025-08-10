use fusio::SeqRead;
use fusio_log::{Decode, Encode};

use super::{schema::DynSchema, DynRecordRef, Value, ValueError};
use crate::record::{error::RecordError, DynamicField, Key, Record};

#[derive(Debug, Clone)]
pub struct DynRecord {
    values: Vec<Value>,
    primary_index: usize,
}

#[allow(unused)]
impl DynRecord {
    /// Create a new DynRecord without validation.
    pub fn new(values: Vec<Value>, primary_index: usize) -> Self {
        Self {
            values,
            primary_index,
        }
    }

    // Used for converting `DynRecord`s to `RecordBatches`
    pub fn schema(&self, primary_index: usize) -> DynSchema {
        let fields = self.values.iter()
            .map(|value| 
                DynamicField::new("".to_string(), value.data_type(), false)
            ).collect();
        DynSchema::new(fields, primary_index)
    }

    /// Create a new DynRecord with validation.
    ///
    /// # Errors
    ///
    /// Returns an error if the validation failed.
    pub fn try_new(values: Vec<Value>, primary_index: usize) -> Result<Self, RecordError> {
        if primary_index >= values.len() {
            return Err(RecordError::InvalidArgumentError(format!(
                "primary key index {} can not great or equal than value length {}",
                primary_index,
                values.len()
            )));
        }

        for (idx, value) in values.iter().enumerate() {
            match value {
                Value::Null if idx == primary_index => {
                    return Err(RecordError::NullNotAllowed(
                        "Primary can not be null".into(),
                    ))
                }
                Value::FixedSizeBinary(v, w) => {
                    if v.len() != *w as usize {
                        return Err(RecordError::ValueError(ValueError::TypeMismatch {
                            expected: format!("FixedSizeBinary({w})"),
                            actual: format!("FixedSizeBinary({})", v.len()),
                        }));
                    }
                }
                _ => (),
            }
        }

        Ok(Self {
            values,
            primary_index,
        })
    }
}

impl Decode for DynRecord {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: SeqRead,
    {
        let len = u32::decode(reader).await? as usize;
        let primary_index = u32::decode(reader).await? as usize;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            let col = Value::decode(reader).await?;
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
        for col in self.values.iter() {
            columns.push(col.as_key_ref());
        }
        DynRecordRef::new(columns, self.primary_index)
    }

    fn size(&self) -> usize {
        self.values.iter().fold(0, |acc, col| acc + col.size())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::io::{Cursor, SeekFrom};

    use arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use super::{DynRecord, DynSchema, Record};
    use crate::{
        make_dyn_schema,
        record::{DynRecordRef, TimeUnit, Value, ValueRef},
    };

    #[allow(unused)]
    pub(crate) fn test_dyn_item_schema() -> DynSchema {
        make_dyn_schema!(
            ("id", DataType::Int64, false),
            ("age", DataType::Int8, true),
            ("height", DataType::Int16, true),
            ("weight", DataType::Int32, false),
            ("name", DataType::Utf8, false),
            ("email", DataType::Utf8, true),
            ("enabled", DataType::Boolean, false),
            ("bytes", DataType::Binary, true),
            ("grade", DataType::Float32, false),
            ("price", DataType::Float64, true),
            (
                "timestamp",
                DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                true
            ),
            0
        )
    }

    #[allow(unused)]
    pub(crate) fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let values = vec![
                Value::Int64(i as i64),
                Value::Int8(i as i8),
                Value::Int16(i as i16 * 20),
                Value::Int32(i * 200_i32),
                Value::String(i.to_string()),
                Value::String(format!("{}@tonbo.io", i)),
                Value::Boolean(i % 2 == 0),
                Value::Binary(i.to_le_bytes().to_vec()),
                Value::Float32(i as f32 * 1.11),
                Value::Float64(i as f64 * 1.01),
                Value::Timestamp(i as i64, TimeUnit::Millisecond),
            ];
            let mut record = DynRecord::new(values, 0);

            if i >= 45 {
                record.values[2] = Value::Null;
            }

            items.push(record);
        }
        items
    }

    fn test_dyn_record() -> DynRecord {
        let values = vec![
            Value::Int64(10i64),
            Value::Int8(10i8),
            Value::Int16(183i16),
            Value::Int32(56i32),
            Value::String("tonbo".to_string()),
            Value::String("contact@tonbo.io".to_string()),
            Value::Boolean(true),
            Value::Binary(b"hello tonbo".to_vec()),
            Value::Float32(1.1234),
            Value::Float64(1.01),
            Value::Timestamp(1717507203412, TimeUnit::Millisecond),
        ];
        DynRecord::new(values, 0)
    }

    #[test]
    fn test_as_record_ref() {
        let record = test_dyn_record();
        let record_ref = record.as_record_ref();
        let expected = DynRecordRef::new(
            vec![
                ValueRef::Int64(10i64),
                ValueRef::Int8(10i8),
                ValueRef::Int16(183i16),
                ValueRef::Int32(56i32),
                ValueRef::String("tonbo"),
                ValueRef::String("contact@tonbo.io"),
                ValueRef::Boolean(true),
                ValueRef::Binary(b"hello tonbo"),
                ValueRef::Float32(1.1234),
                ValueRef::Float64(1.01),
                ValueRef::Timestamp(1717507203412, TimeUnit::Millisecond),
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

    #[test]
    fn test_create_record() {
        let res = DynRecord::try_new(
            vec![
                Value::UInt64(1),
                Value::String("tonbo".into()),
                Value::Null,
                Value::FixedSizeBinary(vec![1, 2, 3], 3),
            ],
            0,
        );
        assert!(res.is_ok());

        let record = res.unwrap();
        assert_eq!(record.values[0], Value::UInt64(1));
        assert_eq!(record.values[1], Value::String("tonbo".into()));
        assert_eq!(record.values[2], Value::Null);
        assert_eq!(record.values[3], Value::FixedSizeBinary(vec![1, 2, 3], 3));
    }

    #[test]
    fn test_create_record_err() {
        // test FixedSizeBinary width not match
        let res = DynRecord::try_new(vec![Value::FixedSizeBinary(vec![1, 2, 3], 4)], 0);
        assert!(res.is_err());

        // test primary key is null
        let res = DynRecord::try_new(vec![Value::Null], 0);
        assert!(res.is_err());

        // test primary key index >= values.len()
        let res = DynRecord::try_new(vec![Value::Null], 1);
        assert!(res.is_err());
    }
}
