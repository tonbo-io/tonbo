use std::{any::Any, marker::PhantomData, mem, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, ArrowPrimitiveType, AsArray},
    datatypes::{
        Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        Int8Type, Schema as ArrowSchema, Time32MillisecondType, Time32SecondType,
        Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
};
use fusio::Write;
use fusio_log::Encode;

use super::{DataType, DynRecord, Value};
use crate::{
    magic::USER_COLUMN_OFFSET,
    record::{
        option::OptionRecordRef, Date32, Date64, Key, LargeBinary, LargeString, Record, RecordRef,
        Schema, Time32, Time64, TimeUnit, Timestamp, F32, F64,
    },
};

#[derive(Clone)]
pub struct DynRecordRef<'r> {
    pub columns: Vec<Value>,
    // XXX: log encode should keep the same behavior
    pub primary_index: usize,
    _marker: PhantomData<&'r ()>,
}

impl DynRecordRef<'_> {
    pub(crate) fn new(columns: Vec<Value>, primary_index: usize) -> Self {
        Self {
            columns,
            primary_index,
            _marker: PhantomData,
        }
    }
}

impl Encode for DynRecordRef<'_> {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        (self.columns.len() as u32).encode(writer).await?;
        (self.primary_index as u32).encode(writer).await?;
        for col in self.columns.iter() {
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

impl<'r> DynRecordRef<'r> {
    fn primitive_value<T>(
        col: &ArrayRef,
        offset: usize,
        idx: usize,
        projection_mask: &'r parquet::arrow::ProjectionMask,
        primary: bool,
    ) -> Arc<dyn Any + Send + Sync>
    where
        T: ArrowPrimitiveType,
    {
        let v = col.as_primitive::<T>();

        if primary {
            Arc::new(v.value(offset)) as Arc<dyn Any + Send + Sync>
        } else {
            let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                .then_some(v.value(offset));
            Arc::new(value) as Arc<dyn Any + Send + Sync>
        }
    }
}

macro_rules! implement_record_ref {
    (
        { $( { $primitive_ty:ty, $primitive_pat:pat, $arrow_ty:ty } ),* $(,)? },
        { $( { $alt_ty2:ty, $alt_variant2:pat,$as_array2:ident, $arrow_ty3:ty, $new_fn:expr} ),* }
    ) => {
        impl<'r> RecordRef<'r> for DynRecordRef<'r> {
            type Record = DynRecord;

            fn key(self) -> <<<Self::Record as Record>::Schema as Schema>::Key as Key>::Ref<'r> {
                self.columns
                    .get(self.primary_index)
                    .cloned()
                    .expect("The primary key must exist")
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

                let schema = record_batch.schema();
                let flattened_fields = schema.flattened_fields();

                for (idx, field) in full_schema.flattened_fields().iter().enumerate().skip(2) {
                    let datatype = DataType::from(field.data_type());
                    let batch_field = flattened_fields
                        .iter()
                        .enumerate()
                        .find(|(_idx, f)| field.contains(f));
                    if batch_field.is_none() {
                        columns.push(Value::with_none_value(
                            datatype,
                            field.name().to_owned(),
                            field.is_nullable(),
                        ));
                        continue;
                    }
                    let col = record_batch.column(batch_field.unwrap().0);
                    let is_nullable = field.is_nullable();
                    let value = match datatype {
                        $(
                            $primitive_pat => Self::primitive_value::<$arrow_ty>(
                                col,
                                offset,
                                idx,
                                projection_mask,
                                primary_index == idx - 2,
                            ),
                        )*
                        DataType::Boolean => {
                            let v = col.as_boolean();

                            if primary_index == idx - 2 {
                                Arc::new(v.value(offset)) as Arc<dyn Any + Send + Sync>
                            } else {
                                let value = (!v.is_null(offset)
                                    && projection_mask.leaf_included(idx))
                                .then_some(v.value(offset));
                                Arc::new(value) as Arc<dyn Any + Send + Sync>
                            }
                        }
                        $(
                            $alt_variant2 => {
                                let array = col.$as_array2::<$arrow_ty3>();
                                if primary_index == idx - 2 {
                                    Arc::new($new_fn(array.value(offset))) as Arc<dyn Any + Send + Sync>
                                } else {
                                    let value = (!array.is_null(offset)
                                        && projection_mask.leaf_included(idx))
                                    .then_some($new_fn(array.value(offset)));
                                    Arc::new(value) as Arc<dyn Any + Send + Sync>
                                }
                            },
                        )*
                        DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                    };
                    columns.push(Value::new(
                        datatype,
                        field.name().to_owned(),
                        value,
                        is_nullable,
                    ));
                }

                let record = DynRecordRef {
                    columns,
                    primary_index,
                    _marker: PhantomData,
                };
                OptionRecordRef::new(ts, record, null)
            }

            fn projection(&mut self, projection_mask: &parquet::arrow::ProjectionMask) {
                for (idx, col) in self.columns.iter_mut().enumerate() {
                    if idx != self.primary_index && !projection_mask.leaf_included(idx + USER_COLUMN_OFFSET)
                    {
                        match col.datatype() {
                            $(
                                $primitive_pat => col.value = Arc::<Option<$primitive_ty>>::new(None),
                            )*
                            DataType::Boolean => col.value = Arc::<Option<bool>>::new(None),
                            $(
                                $alt_variant2 => col.value = Arc::<Option<$alt_ty2>>::new(None),
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

    use parquet::arrow::{ArrowSchemaConverter, ProjectionMask};

    use crate::{
        cast_arc_value, dyn_record, dyn_schema, make_dyn_record, make_dyn_schema,
        record::{DataType, Record, RecordRef, Schema, TimeUnit, Timestamp, F32, F64},
    };

    #[test]
    fn test_float_projection() {
        let schema = dyn_schema!(
            ("_null", Boolean, false),
            ("ts", UInt32, false),
            ("id", Float64, false),
            ("foo", Float32, false),
            ("foo_opt", Float32, true),
            ("bar", Float64, false),
            ("bar_opt", Float64, true),
            2
        );
        let record = dyn_record!(
            ("_null", Boolean, false, true),
            ("ts", UInt32, false, 7u32),
            ("id", Float64, false, F64::from(1.23)),
            ("foo", Float32, false, F32::from(1.23)),
            ("foo_opt", Float32, true, None::<F32>),
            ("bar", Float64, false, F64::from(3.234)),
            ("bar_opt", Float64, true, Some(F64::from(13.234))),
            2
        );
        {
            // test project all
            let mut record_ref = record.as_record_ref();
            record_ref.projection(&ProjectionMask::all());
            let columns = record_ref.columns;
            assert_eq!(*cast_arc_value!(columns[0].value, Option<bool>), Some(true));
            assert_eq!(*cast_arc_value!(columns[1].value, Option<u32>), Some(7u32));
            assert_eq!(*cast_arc_value!(columns[2].value, F64), 1.23.into());
            assert_eq!(
                *cast_arc_value!(columns[3].value, Option<F32>),
                Some(1.23.into())
            );
            assert_eq!(*cast_arc_value!(columns[4].value, Option<F32>), None,);
            assert_eq!(
                *cast_arc_value!(columns[5].value, Option<F64>),
                Some(3.234.into())
            );
            assert_eq!(
                *cast_arc_value!(columns[6].value, Option<F64>),
                Some(13.234.into())
            );
        }
        {
            // test project no columns
            let mut record_ref = record.as_record_ref();
            let mask = ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(schema.arrow_schema())
                    .unwrap(),
                vec![1],
            );
            record_ref.projection(&mask);
            let columns = record_ref.columns;
            assert_eq!(*cast_arc_value!(columns[0].value, Option<bool>), None);
            assert_eq!(*cast_arc_value!(columns[1].value, Option<u32>), None);
            assert_eq!(*cast_arc_value!(columns[2].value, F64), 1.23.into());
            assert_eq!(*cast_arc_value!(columns[3].value, Option<F32>), None);
            assert_eq!(*cast_arc_value!(columns[4].value, Option<F32>), None);
            assert_eq!(*cast_arc_value!(columns[5].value, Option<F64>), None);
            assert_eq!(*cast_arc_value!(columns[6].value, Option<F64>), None);
        }
    }

    #[test]
    fn test_string_projection() {
        let schema = dyn_schema!(
            ("_null", Boolean, false),
            ("ts", UInt32, false),
            ("id", String, false),
            ("name", String, false),
            ("email", String, true),
            ("adress", String, true),
            ("data", Bytes, true),
            2
        );
        let record = dyn_record!(
            ("_null", Boolean, false, true),
            ("ts", UInt32, false, 7u32),
            ("id", String, false, "abcd".to_string()),
            ("name", String, false, "Jack".to_string()),
            ("email", String, true, Some("abc@tonbo.io".to_string())),
            ("adress", String, true, None::<String>),
            ("data", Bytes, true, Some(b"hello,tonbo".to_vec())),
            2
        );
        {
            // test project all
            let mut record_ref = record.as_record_ref();
            record_ref.projection(&ProjectionMask::all());
            let columns = record_ref.columns;
            assert_eq!(*cast_arc_value!(columns[0].value, Option<bool>), Some(true));
            assert_eq!(*cast_arc_value!(columns[1].value, Option<u32>), Some(7u32));
            assert_eq!(cast_arc_value!(columns[2].value, String), "abcd");
            assert_eq!(
                *cast_arc_value!(columns[3].value, Option<String>),
                Some("Jack".into()),
            );
            assert_eq!(
                *cast_arc_value!(columns[4].value, Option<String>),
                Some("abc@tonbo.io".into())
            );
            cast_arc_value!(columns[6].value, Option<Vec<u8>>);
            assert_eq!(
                *cast_arc_value!(columns[6].value, Option<Vec<u8>>),
                Some(b"hello,tonbo".to_vec())
            );
        }
        {
            // test project no columns
            let mut record_ref = record.as_record_ref();
            let mask = ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(schema.arrow_schema())
                    .unwrap(),
                vec![1],
            );
            record_ref.projection(&mask);
            let columns = record_ref.columns;
            assert_eq!(*cast_arc_value!(columns[0].value, Option<bool>), None);
            assert_eq!(*cast_arc_value!(columns[1].value, Option<u32>), None);
            assert_eq!(cast_arc_value!(columns[2].value, String), "abcd");
            assert_eq!(*cast_arc_value!(columns[3].value, Option<String>), None,);
            assert_eq!(*cast_arc_value!(columns[4].value, Option<String>), None,);
            assert_eq!(*cast_arc_value!(columns[5].value, Option<String>), None,);
            assert_eq!(*cast_arc_value!(columns[6].value, Option<Vec<u8>>), None);
        }
    }

    #[test]
    fn test_timestamp_projection() {
        let schema = make_dyn_schema!(
            ("_null", DataType::Boolean, false),
            ("_ts", DataType::UInt32, false),
            ("id", DataType::Timestamp(TimeUnit::Millisecond), false),
            ("ts1", DataType::Timestamp(TimeUnit::Millisecond), false),
            ("ts2", DataType::Timestamp(TimeUnit::Millisecond), true),
            ("ts3", DataType::Timestamp(TimeUnit::Millisecond), true),
            2
        );
        let record = make_dyn_record!(
            ("_null", DataType::Boolean, false, true),
            ("_ts", DataType::UInt32, false, 7u32),
            (
                "id",
                DataType::Timestamp(TimeUnit::Millisecond),
                false,
                Timestamp::new_millis(1717507203412)
            ),
            (
                "ts1",
                DataType::Timestamp(TimeUnit::Millisecond),
                false,
                Timestamp::new_millis(1717507203432)
            ),
            (
                "ts2",
                DataType::Timestamp(TimeUnit::Millisecond),
                true,
                Some(Timestamp::new_millis(1717507203442))
            ),
            (
                "ts3",
                DataType::Timestamp(TimeUnit::Millisecond),
                true,
                None::<Timestamp>
            ),
            2
        );
        {
            // test project all
            let mut record_ref = record.as_record_ref();
            record_ref.projection(&ProjectionMask::all());
            let columns = record_ref.columns;
            assert_eq!(
                cast_arc_value!(columns[2].value, Timestamp),
                &Timestamp::new_millis(1717507203412)
            );
            assert_eq!(
                cast_arc_value!(columns[3].value, Option<Timestamp>),
                &Some(Timestamp::new_millis(1717507203432))
            );
            assert_eq!(
                cast_arc_value!(columns[4].value, Option<Timestamp>),
                &Some(Timestamp::new_millis(1717507203442))
            );

            assert_eq!(*cast_arc_value!(columns[5].value, Option<Timestamp>), None);
        }
        {
            // test project no columns
            let mut record_ref = record.as_record_ref();
            let mask = ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(schema.arrow_schema())
                    .unwrap(),
                vec![1],
            );
            record_ref.projection(&mask);
            let columns = record_ref.columns;
            assert_eq!(
                *cast_arc_value!(columns[2].value, Timestamp),
                Timestamp::new_millis(1717507203412)
            );
            assert_eq!(*cast_arc_value!(columns[3].value, Option<Timestamp>), None);
            assert_eq!(*cast_arc_value!(columns[4].value, Option<Timestamp>), None);
            assert_eq!(*cast_arc_value!(columns[5].value, Option<Timestamp>), None);
        }
    }
}
