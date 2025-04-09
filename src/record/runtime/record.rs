use std::sync::Arc;

use fusio::SeqRead;
use fusio_log::{Decode, Encode};

use super::{schema::DynSchema, wrapped_value, DataType, DynRecordRef, Value};
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
                    DataType::List(desc) => match desc.datatype {
                        DataType::UInt8 => {
                            Arc::new(cast_arc_value!(col.value, Option<Vec<u8>>).clone().unwrap())
                        }
                        DataType::UInt16 => Arc::new(
                            cast_arc_value!(col.value, Option<Vec<u16>>)
                                .clone()
                                .unwrap(),
                        ),
                        DataType::UInt32 => Arc::new(
                            cast_arc_value!(col.value, Option<Vec<u32>>)
                                .clone()
                                .unwrap(),
                        ),
                        DataType::UInt64 => Arc::new(
                            cast_arc_value!(col.value, Option<Vec<u64>>)
                                .clone()
                                .unwrap(),
                        ),
                        DataType::Int8 => {
                            Arc::new(cast_arc_value!(col.value, Option<Vec<i8>>).clone().unwrap())
                        }
                        DataType::Int16 => Arc::new(
                            cast_arc_value!(col.value, Option<Vec<i16>>)
                                .clone()
                                .unwrap(),
                        ),
                        DataType::Int32 => Arc::new(
                            cast_arc_value!(col.value, Option<Vec<i32>>)
                                .clone()
                                .unwrap(),
                        ),
                        DataType::Int64 => Arc::new(
                            cast_arc_value!(col.value, Option<Vec<i64>>)
                                .clone()
                                .unwrap(),
                        ),
                        DataType::String => Arc::new(
                            cast_arc_value!(col.value, Option<Vec<String>>)
                                .clone()
                                .unwrap(),
                        ),
                        DataType::Boolean => Arc::new(
                            cast_arc_value!(col.value, Option<Vec<bool>>)
                                .clone()
                                .unwrap(),
                        ),
                        DataType::Bytes => Arc::new(
                            cast_arc_value!(col.value, Option<Vec<Vec<u8>>>)
                                .clone()
                                .unwrap(),
                        ),
                        DataType::List(_) => {
                            unimplemented!("Vec<Vec<T>> is not supporte yet")
                        }
                    },
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
            let value = if idx != self.primary_index && !is_nullable {
                wrapped_value(datatype, &col.value)
            } else {
                col.value.clone()
            };

            columns.push(Value::new(
                datatype.clone(),
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
    use std::{io::Cursor, sync::Arc};

    use fusio_log::{Decode, Encode};

    use super::{DynRecord, DynSchema};
    use crate::{
        dyn_schema,
        record::{DataType, Record, Value, ValueDesc},
    };

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

    #[test]
    fn test_dyn_record_ref() {
        {
            let desc = ValueDesc::new("".into(), DataType::String, false);
            let record = DynRecord::new(
                vec![
                    Value::new(DataType::UInt32, "id".into(), Arc::new(1_u32), false),
                    Value::new(
                        DataType::List(Arc::new(desc.clone())),
                        "strs".to_string(),
                        Arc::new(vec![
                            "abc".to_string(),
                            "xyz".to_string(),
                            "tonbo".to_string(),
                        ]),
                        false,
                    ),
                    Value::new(
                        DataType::List(Arc::new(desc.clone())),
                        "strs_option".to_string(),
                        Arc::new(Some(vec![
                            "abc".to_string(),
                            "xyz".to_string(),
                            "tonbo".to_string(),
                        ])),
                        true,
                    ),
                ],
                0,
            );

            let record_ref = record.as_record_ref();
            assert_eq!(
                record_ref.columns.get(1).unwrap(),
                &Value::new(
                    DataType::List(Arc::new(desc.clone())),
                    "strs".to_string(),
                    Arc::new(Some(vec![
                        "abc".to_string(),
                        "xyz".to_string(),
                        "tonbo".to_string(),
                    ])),
                    false,
                )
            );
            assert_eq!(
                record_ref.columns.get(2).unwrap(),
                &Value::new(
                    DataType::List(Arc::new(desc.clone())),
                    "strs".to_string(),
                    Arc::new(Some(vec![
                        "abc".to_string(),
                        "xyz".to_string(),
                        "tonbo".to_string(),
                    ])),
                    true,
                )
            )
        }
        {
            let record = DynRecord::new(
                vec![
                    Value::new(DataType::UInt32, "id".into(), Arc::new(1_u32), false),
                    Value::new(
                        DataType::List(Arc::new(ValueDesc::new(
                            "".into(),
                            DataType::Boolean,
                            false,
                        ))),
                        "bools".to_string(),
                        Arc::new(vec![true, false, false, true]),
                        false,
                    ),
                    Value::new(
                        DataType::List(Arc::new(ValueDesc::new("".into(), DataType::Bytes, true))),
                        "bytes".to_string(),
                        Arc::new(Some(vec![
                            vec![0_u8, 1, 2, 3],
                            vec![10, 11, 12, 13],
                            vec![20, 21, 22, 23],
                        ])),
                        true,
                    ),
                ],
                0,
            );

            let record_ref = record.as_record_ref();
            assert_eq!(
                record_ref.columns.get(1).unwrap(),
                &Value::new(
                    DataType::List(Arc::new(ValueDesc::new(
                        "".into(),
                        DataType::Boolean,
                        false,
                    ))),
                    "bools".to_string(),
                    Arc::new(Some(vec![true, false, false, true])),
                    false,
                )
            );
            assert_eq!(
                record_ref.columns.get(2).unwrap(),
                &Value::new(
                    DataType::List(Arc::new(ValueDesc::new("".into(), DataType::Bytes, true))),
                    "bytes".to_string(),
                    Arc::new(Some(vec![
                        vec![0_u8, 1, 2, 3],
                        vec![10, 11, 12, 13],
                        vec![20, 21, 22, 23],
                    ])),
                    true,
                )
            )
        }
    }

    #[tokio::test]
    async fn test_encode_encode_dyn_record_list() {
        use tokio::io::AsyncSeekExt;

        {
            let desc = ValueDesc::new("".into(), DataType::String, false);
            let record = DynRecord::new(
                vec![
                    Value::new(DataType::UInt32, "id".into(), Arc::new(1_u32), false),
                    Value::new(
                        DataType::List(Arc::new(desc.clone())),
                        "strs".to_string(),
                        Arc::new(vec![
                            "abc".to_string(),
                            "xyz".to_string(),
                            "tonbo".to_string(),
                        ]),
                        false,
                    ),
                    Value::new(
                        DataType::List(Arc::new(desc.clone())),
                        "strs_option".to_string(),
                        Arc::new(Some(vec![
                            "abc".to_string(),
                            "xyz".to_string(),
                            "tonbo".to_string(),
                        ])),
                        true,
                    ),
                ],
                0,
            );

            let mut source = vec![];
            let mut cursor = Cursor::new(&mut source);
            let record_ref = record.as_record_ref();
            record_ref.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = DynRecord::decode(&mut cursor).await.unwrap();
            assert_eq!(decoded.values, record.values);
        }

        {
            let record = DynRecord::new(
                vec![
                    Value::new(DataType::UInt32, "id".into(), Arc::new(1_u32), false),
                    Value::new(
                        DataType::List(Arc::new(ValueDesc::new(
                            "".into(),
                            DataType::Boolean,
                            false,
                        ))),
                        "bools".to_string(),
                        Arc::new(vec![true, false, false, false, true]),
                        false,
                    ),
                    Value::new(
                        DataType::List(Arc::new(ValueDesc::new("".into(), DataType::Bytes, false))),
                        "bytes".to_string(),
                        Arc::new(Some(vec![
                            vec![1_u8, 2, 3, 4],
                            vec![11, 22, 23, 24],
                            vec![31, 32, 33, 34],
                        ])),
                        true,
                    ),
                    Value::new(
                        DataType::List(Arc::new(ValueDesc::new("".into(), DataType::Int64, true))),
                        "none".to_string(),
                        Arc::new(None::<Vec<i64>>),
                        true,
                    ),
                ],
                0,
            );

            let mut source = vec![];
            let mut cursor = Cursor::new(&mut source);
            let record_ref = record.as_record_ref();
            record_ref.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = DynRecord::decode(&mut cursor).await.unwrap();
            assert_eq!(decoded.values, record.values);
        }
    }
}
