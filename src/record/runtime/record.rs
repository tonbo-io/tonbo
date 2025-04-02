use std::sync::Arc;

use fusio::SeqRead;
use fusio_log::{Decode, Encode};

use super::{schema::DynSchema, DataType, DynRecordRef, Value};
use crate::{
    cast_arc_value,
    record::{Record, RecordDecodeError},
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
                    DataType::String => {
                        Arc::new(cast_arc_value!(col.value, Option<String>).clone().unwrap())
                    }
                    DataType::Boolean => {
                        Arc::new(cast_arc_value!(col.value, Option<bool>).unwrap())
                    }
                    DataType::Bytes => {
                        Arc::new(cast_arc_value!(col.value, Option<Vec<u8>>).clone().unwrap())
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
                    DataType::String => {
                        Arc::new(Some(cast_arc_value!(col.value, String).to_owned()))
                    }
                    DataType::Boolean => Arc::new(Some(*cast_arc_value!(col.value, bool))),
                    DataType::Bytes => {
                        Arc::new(Some(cast_arc_value!(col.value, Vec<u8>).to_owned()))
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

#[cfg(test)]
pub(crate) mod test {
    use std::sync::Arc;

    use super::{DynRecord, DynSchema};
    use crate::dyn_schema;

    #[allow(unused)]
    pub(crate) fn test_dyn_item_schema() -> DynSchema {
        dyn_schema!(
            ("id", Int64, false),
            ("age", Int8, true),
            ("height", Int16, true),
            ("weight", Int32, false),
            ("name", String, false),
            ("email", String, true),
            ("enabled", Boolean, false),
            ("bytes", Bytes, true),
            0
        )
    }

    #[allow(unused)]
    pub(crate) fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let mut record = dyn_record!(
                ("id", Int64, false, i as i64),
                ("age", Int8, true, Some(i as i8)),
                ("height", Int16, true, Some(i as i16 * 20)),
                ("weight", Int32, false, i * 200_i32),
                ("name", String, false, i.to_string()),
                ("email", String, true, Some(format!("{}@tonbo.io", i))),
                ("enabled", Boolean, false, i % 2 == 0),
                ("bytes", Bytes, true, Some(i.to_le_bytes().to_vec())),
                0
            );
            if i >= 45 {
                record.values[2].value = Arc::<Option<i16>>::new(None);
            }

            items.push(record);
        }
        items
    }
}
