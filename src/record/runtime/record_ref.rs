use std::{marker::PhantomData, mem, sync::Arc};

use arrow::{
    array::{Array, AsArray},
    datatypes::{
        Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        Int8Type, Schema as ArrowSchema, Time32MillisecondType, Time32SecondType,
        Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
};
use common::{
    datatype::DataType, Date32, Date64, Key, LargeBinary, LargeString, PrimaryKey, Time32, Time64,
    TimeUnit, Timestamp, Value, F32, F64,
};
use fusio::Write;
use fusio_log::Encode;

use super::{null_value, DynRecord};
use crate::{
    magic::USER_COLUMN_OFFSET,
    record::{option::OptionRecordRef, Record, RecordRef},
};

#[derive(Clone)]
pub struct DynRecordRef<'r> {
    pub columns: Vec<Arc<dyn Value>>,
    // TODO: replace mask with bitmap.
    pub opt_mask: Vec<bool>,
    // XXX: log encode should keep the same behavior
    pub primary_index: usize,
    _marker: PhantomData<&'r ()>,
}

impl<'r> DynRecordRef<'r> {
    pub(crate) fn new(columns: Vec<Arc<dyn Value>>, primary_index: usize) -> Self {
        let opt_mask = columns
            .iter()
            .map(|col| col.is_none() || col.is_some())
            .collect::<Vec<bool>>();

        Self {
            columns,
            primary_index,
            opt_mask,
            _marker: PhantomData,
        }
    }
}

impl<'r> Encode for DynRecordRef<'r> {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        (self.columns.len() as u32).encode(writer).await?;
        (self.primary_index as u32).encode(writer).await?;
        for (col, is_option) in self.columns.iter().zip(self.opt_mask.iter()) {
            is_option.encode(writer).await?;
            col.encode(writer).await?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let mut size = 2 * mem::size_of::<u32>();
        for col in self.columns.iter() {
            size += col.size();
        }
        size
    }
}

macro_rules! implement_record_ref {
    (
        { $( { $primitive_ty:ty, $primitive_pat:pat, $arrow_ty:ty } ),* $(,)? },
        { $( { $alt_ty2:ty, $alt_variant2:pat,$as_array2:ident, $arrow_ty3:ty, $new_fn:expr} ),* }
    ) => {
        impl<'r> RecordRef<'r> for DynRecordRef<'r> {
            type Record = DynRecord;

            fn key(self) -> PrimaryKey {
                PrimaryKey::new(
                    vec![
                        self.columns
                            .get(self.primary_index)
                            .cloned()
                            .expect("The primary key must exist")
                    ]
                )
            }

            fn from_record_batch(
                record_batch: &'r arrow::array::RecordBatch,
                offset: usize,
                projection_mask: &'r parquet::arrow::ProjectionMask,
                full_schema: &'r Arc<ArrowSchema>,
            ) -> OptionRecordRef<'r, Self> {
                let null = record_batch.column(0).as_boolean().value(offset);
                let metadata = full_schema.metadata();

                let primary_index = metadata
                    .get("primary_key_index")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let ts = record_batch
                    .column(1)
                    .as_primitive::<arrow::datatypes::UInt32Type>()
                    .value(offset)
                    .into();

                let mut columns = vec![];
                let mut opt_mask = vec![];

                let schema = record_batch.schema();
                let flattened_fields = schema.flattened_fields();

                for (idx, field) in full_schema.flattened_fields().iter().enumerate().skip(2) {
                    let datatype = DataType::from(field.data_type());
                    let batch_field = flattened_fields
                        .iter()
                        .enumerate()
                        .find(|(_idx, f)| field.contains(f));
                    if batch_field.is_none() {
                        columns.push(null_value(&datatype));
                        continue;
                    }
                    let col = record_batch.column(batch_field.unwrap().0);
                    let is_nullable = field.is_nullable();
                    let value: Arc<dyn Value> = match datatype {
                        $(
                            $primitive_pat => {

                                let v = col.as_primitive::<$arrow_ty>();

                                if primary_index == idx - 2 {
                                    Arc::new(v.value(offset))
                                } else {
                                    let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                                        .then_some(v.value(offset));
                                    Arc::new(value)
                                }

                            }
                        )*
                        DataType::Boolean => {
                            let v = col.as_boolean();

                            if primary_index == idx - 2 {
                                Arc::new(v.value(offset))
                            } else {
                                let value = (!v.is_null(offset)
                                    && projection_mask.leaf_included(idx))
                                .then_some(v.value(offset));
                                Arc::new(value)
                            }
                        }
                        $(
                            $alt_variant2 => {
                                let array = col.$as_array2::<$arrow_ty3>();
                                if primary_index == idx - 2 {
                                    Arc::new($new_fn(array.value(offset)))
                                } else {
                                    let value = (!array.is_null(offset)
                                        && projection_mask.leaf_included(idx))
                                    .then_some($new_fn(array.value(offset)));
                                    Arc::new(value)
                                }
                            },
                        )*
                        DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                    };
                    opt_mask.push(is_nullable);
                    columns.push(value);
                }

                let record = DynRecordRef {
                    columns,
                    primary_index,
                    opt_mask,
                    _marker: PhantomData,
                };
                OptionRecordRef::new(ts, record, null)
            }

            fn projection(&mut self, projection_mask: &parquet::arrow::ProjectionMask) {
                for (idx, col) in self.columns.iter_mut().enumerate() {
                    if idx != self.primary_index && !projection_mask.leaf_included(idx + USER_COLUMN_OFFSET)
                    {
                        *col = match col.data_type() {
                            $(
                                $primitive_pat => Arc::<Option<$primitive_ty>>::new(None) as Arc<dyn Value>,
                            )*
                            DataType::Boolean => Arc::<Option<bool>>::new(None) as Arc<dyn Value>,
                            $(
                                $alt_variant2 => Arc::<Option<$alt_ty2>>::new(None) as Arc<dyn Value>,
                            )*
                            DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                        };
                    }
                }
            }
        }
    };
}

implement_record_ref!(
    {
        // primitive_ty type
        { u8, DataType::UInt8, UInt8Type },
        { u16, DataType::UInt16, UInt16Type },
        { u32, DataType::UInt32, UInt32Type },
        { u64, DataType::UInt64, UInt64Type },
        { i8, DataType::Int8, Int8Type },
        { i16, DataType::Int16, Int16Type },
        { i32, DataType::Int32, Int32Type },
        { i64, DataType::Int64, Int64Type },
    },
    //  bool is special case, it is handled separately
    {
        // { tonbo_type, DataType::xxx, as_array_method, inner_type_to_as_array_method, one_param_constructor }
        { String, DataType::String, as_string, i32, String::from },
        { LargeString, DataType::LargeString, as_string, i64, String::from },
        { Vec<u8>, DataType::Bytes, as_binary, i32, Vec::from },
        { LargeBinary, DataType::LargeBinary, as_binary, i64, Vec::from },
        { F32, DataType::Float32, as_primitive, Float32Type, F32::from },
        { F64, DataType::Float64, as_primitive, Float64Type, F64::from },
        { Date32, DataType::Date32, as_primitive, Date32Type, Date32::new },
        { Date64, DataType::Date64, as_primitive, Date64Type, Date64::new },
        { Timestamp, DataType::Timestamp(TimeUnit::Second), as_primitive, TimestampSecondType, Timestamp::new_seconds },
        { Timestamp, DataType::Timestamp(TimeUnit::Millisecond), as_primitive, TimestampMillisecondType, Timestamp::new_millis },
        { Timestamp, DataType::Timestamp(TimeUnit::Microsecond), as_primitive, TimestampMicrosecondType, Timestamp::new_micros },
        { Timestamp, DataType::Timestamp(TimeUnit::Nanosecond), as_primitive, TimestampNanosecondType, Timestamp::new_nanos },
        { Time32, DataType::Time32(TimeUnit::Second), as_primitive, Time32SecondType, Time32::new_seconds },
        { Time32, DataType::Time32(TimeUnit::Millisecond), as_primitive,  Time32MillisecondType, Time32::new_millis },
        { Time64, DataType::Time64(TimeUnit::Microsecond),as_primitive, Time64MicrosecondType, Time64::new_micros },
        { Time64, DataType::Time64(TimeUnit::Nanosecond),as_primitive, Time64NanosecondType, Time64::new_nanos }
    }
);

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit as ArrowTimeUnit};
    use common::{AsValue, Timestamp, F32, F64};
    use parquet::arrow::{ArrowSchemaConverter, ProjectionMask};

    use crate::{
        dyn_record, make_dyn_record,
        record::{Record, RecordRef},
    };

    #[test]
    fn test_float_projection() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("_null", ArrowDataType::Boolean, false),
            Field::new("ts", ArrowDataType::UInt32, false),
            Field::new("id", ArrowDataType::Float64, false),
            Field::new("foo", ArrowDataType::Float32, false),
            Field::new("foo_opt", ArrowDataType::Float32, true),
            Field::new("bar", ArrowDataType::Float64, false),
            Field::new("bar_opt", ArrowDataType::Float64, true),
        ]));
        let record = dyn_record!(
            ("id", Float64, false, Arc::new(F64::from(1.23))),
            ("foo", Float32, false, Arc::new(F32::from(1.23))),
            ("foo_opt", Float32, true, Arc::new(None::<F32>)),
            ("bar", Float64, false, Arc::new(F64::from(3.234))),
            ("bar_opt", Float64, true, Arc::new(Some(F64::from(13.234)))),
            0
        );
        {
            // test project all
            let mut record_ref = record.as_record_ref();
            record_ref.projection(&ProjectionMask::all());
            let columns = record_ref.columns;
            assert_eq!(columns[0].as_f64(), &1.23.into());
            assert_eq!(columns[1].as_f32_opt(), &Some(1.23.into()));
            assert_eq!(columns[2].as_f32_opt(), &None,);
            assert_eq!(columns[3].as_f64_opt(), &Some(3.234.into()));
            assert_eq!(columns[4].as_f64_opt(), &Some(13.234.into()));
        }
        {
            // test project no columns
            let mut record_ref = record.as_record_ref();
            let mask = ProjectionMask::roots(
                &ArrowSchemaConverter::new().convert(&arrow_schema).unwrap(),
                vec![1],
            );
            record_ref.projection(&mask);
            let columns = record_ref.columns;
            assert_eq!(columns[0].as_f64(), &1.23.into());
            assert_eq!(columns[1].as_f32_opt(), &None);
            assert_eq!(columns[2].as_f32_opt(), &None);
            assert_eq!(columns[3].as_f64_opt(), &None);
            assert_eq!(columns[4].as_f64_opt(), &None);
        }
    }

    #[test]
    fn test_string_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_null", ArrowDataType::Boolean, false),
            Field::new("ts", ArrowDataType::UInt32, false),
            Field::new("id", ArrowDataType::Utf8, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("email", ArrowDataType::Utf8, true),
            Field::new("adress", ArrowDataType::Utf8, true),
            Field::new("data", ArrowDataType::Binary, true),
        ]));
        let record = dyn_record!(
            ("id", String, false, Arc::new("abcd".to_string())),
            ("name", String, false, Arc::new("Jack".to_string())),
            (
                "email",
                String,
                true,
                Arc::new(Some("abc@tonbo.io".to_string()))
            ),
            ("adress", String, true, Arc::new(None::<String>)),
            ("data", Bytes, true, Arc::new(Some(b"hello,tonbo".to_vec()))),
            0
        );
        {
            // test project all
            let mut record_ref = record.as_record_ref();
            record_ref.projection(&ProjectionMask::all());
            let columns = record_ref.columns;
            assert_eq!(columns[0].as_string(), "abcd");
            assert_eq!(columns[1].as_string_opt(), &Some("Jack".to_owned()));
            assert_eq!(columns[2].as_string_opt(), &Some("abc@tonbo.io".into()));
            assert_eq!(columns[3].as_string_opt(), &None);
            assert_eq!(*columns[4].as_bytes_opt(), Some(b"hello,tonbo".to_vec()));
        }
        {
            // test project no columns
            let mut record_ref = record.as_record_ref();
            let mask = ProjectionMask::roots(
                &ArrowSchemaConverter::new().convert(&schema).unwrap(),
                vec![1],
            );
            record_ref.projection(&mask);
            let columns = record_ref.columns;
            assert_eq!(columns[0].as_string(), "abcd");
            assert_eq!(*columns[1].as_string_opt(), None,);
            assert_eq!(*columns[2].as_string_opt(), None,);
            assert_eq!(*columns[3].as_string_opt(), None,);
            assert_eq!(*columns[4].as_bytes_opt(), None);
        }
    }

    #[test]
    fn test_timestamp_projection() {
        let schema = Schema::new(vec![
            Field::new("_null", ArrowDataType::Boolean, false),
            Field::new("ts", ArrowDataType::UInt32, false),
            Field::new(
                "id",
                ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "ts1",
                ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "ts2",
                ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts3",
                ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                true,
            ),
        ]);
        let record = make_dyn_record!(
            (
                "id",
                DataType::Timestamp(TimeUnit::Millisecond),
                false,
                Arc::new(Timestamp::new_millis(1717507203412))
            ),
            (
                "ts1",
                DataType::Timestamp(TimeUnit::Millisecond),
                false,
                Arc::new(Timestamp::new_millis(1717507203432))
            ),
            (
                "ts2",
                DataType::Timestamp(TimeUnit::Millisecond),
                true,
                Arc::new(Some(Timestamp::new_millis(1717507203442)))
            ),
            (
                "ts3",
                DataType::Timestamp(TimeUnit::Millisecond),
                true,
                Arc::new(None::<Timestamp>)
            ),
            0
        );
        {
            // test project all
            let mut record_ref = record.as_record_ref();
            record_ref.projection(&ProjectionMask::all());
            let columns = record_ref.columns;
            assert_eq!(
                columns[0].as_timestamp(),
                &Timestamp::new_millis(1717507203412)
            );
            assert_eq!(
                columns[1].as_timestamp_opt(),
                &Some(Timestamp::new_millis(1717507203432))
            );
            assert_eq!(
                columns[2].as_timestamp_opt(),
                &Some(Timestamp::new_millis(1717507203442))
            );

            assert_eq!(*columns[3].as_timestamp_opt(), None);
        }
        {
            // test project no columns
            let mut record_ref = record.as_record_ref();
            let mask = ProjectionMask::roots(
                &ArrowSchemaConverter::new().convert(&schema).unwrap(),
                vec![0],
            );
            record_ref.projection(&mask);
            let columns = record_ref.columns;
            assert_eq!(
                *columns[0].as_timestamp(),
                Timestamp::new_millis(1717507203412)
            );
            assert_eq!(*columns[1].as_timestamp_opt(), None);
            assert_eq!(*columns[2].as_timestamp_opt(), None);
            assert_eq!(*columns[3].as_timestamp_opt(), None);
        }
    }
}
