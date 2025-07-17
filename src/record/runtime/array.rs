use std::{mem, sync::Arc};

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, ArrowPrimitiveType, BooleanArray, BooleanBufferBuilder,
        BooleanBuilder, Date32Builder, Date64Builder, Float32Builder, Float64Builder,
        GenericBinaryArray, GenericBinaryBuilder, LargeStringArray, LargeStringBuilder,
        PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder, Time32MillisecondArray,
        Time32MillisecondBuilder, Time32SecondArray, Time32SecondBuilder, Time64MicrosecondArray,
        Time64MicrosecondBuilder, Time64NanosecondArray, Time64NanosecondBuilder,
        TimestampMicrosecondArray, TimestampMicrosecondBuilder, TimestampMillisecondArray,
        TimestampMillisecondBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder,
        TimestampSecondArray, TimestampSecondBuilder, UInt32Builder,
    },
    datatypes::{
        DataType as ArrowDataType, Date32Type, Date64Type, Float32Type, Float64Type, Int16Type,
        Int32Type, Int64Type, Int8Type, Schema as ArrowSchema, UInt16Type, UInt32Type, UInt64Type,
        UInt8Type,
    },
};
use common::{
    datatype::DataType, Date32, Date64, LargeBinary, LargeString, PrimaryKey, Time32, Time64,
    TimeUnit, Timestamp, F32, F64,
};

use super::{record::DynRecord, record_ref::DynRecordRef};
use crate::{
    inmem::immutable::{ArrowArrays, Builder},
    magic::USER_COLUMN_OFFSET,
    record::Record,
    timestamp::Ts,
    Value,
};

#[allow(unused)]
pub struct DynRecordImmutableArrays {
    _null: Arc<arrow::array::BooleanArray>,
    _ts: Arc<arrow::array::UInt32Array>,
    columns: Vec<Arc<dyn Array>>,
    record_batch: arrow::record_batch::RecordBatch,
}

pub struct DynRecordBuilder {
    builders: Vec<Box<dyn ArrayBuilder + Send + Sync>>,
    datatypes: Vec<DataType>,
    _null: BooleanBufferBuilder,
    _ts: UInt32Builder,
    schema: Arc<ArrowSchema>,
}

impl DynRecordImmutableArrays {
    fn primitive_value<T>(arr: &ArrayRef, offset: usize) -> T::Native
    where
        T: ArrowPrimitiveType,
    {
        arr.as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap()
            .value(offset)
    }

    fn null_value(&self, index: usize) -> Arc<dyn Value> {
        match self.columns[index].data_type() {
            ArrowDataType::Boolean => Arc::new(None::<bool>),
            ArrowDataType::Int8 => Arc::new(None::<i8>),
            ArrowDataType::Int16 => Arc::new(None::<i16>),
            ArrowDataType::Int32 => Arc::new(None::<i32>),
            ArrowDataType::Int64 => Arc::new(None::<i64>),
            ArrowDataType::UInt8 => Arc::new(None::<u8>),
            ArrowDataType::UInt16 => Arc::new(None::<u16>),
            ArrowDataType::UInt32 => Arc::new(None::<u32>),
            ArrowDataType::UInt64 => Arc::new(None::<u64>),
            ArrowDataType::Float32 => Arc::new(None::<F32>),
            ArrowDataType::Float64 => Arc::new(None::<F64>),
            ArrowDataType::Timestamp(_, _) => Arc::new(None::<Timestamp>),
            ArrowDataType::Date32 => Arc::new(None::<Date32>),
            ArrowDataType::Date64 => Arc::new(None::<Date64>),
            ArrowDataType::Time32(_) => Arc::new(None::<Time32>),
            ArrowDataType::Time64(_) => Arc::new(None::<Time64>),
            ArrowDataType::Binary | ArrowDataType::LargeBinary => Arc::new(None::<Vec<u8>>),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Arc::new(None::<String>),
            _ => todo!(),
        }
    }
}

impl DynRecordBuilder {
    fn as_builder<T>(builder: &dyn ArrayBuilder) -> &T
    where
        T: ArrayBuilder,
    {
        builder.as_any().downcast_ref::<T>().unwrap()
    }

    fn as_builder_mut<T>(builder: &mut dyn ArrayBuilder) -> &mut T
    where
        T: ArrayBuilder,
    {
        builder.as_any_mut().downcast_mut::<T>().unwrap()
    }
}

macro_rules! implement_arrow_array {
    (
        { $( { $primitive_ty:ty, $primitive_tonbo_ty:ty, $primitive_pat:pat, $arrow_ty:ty } ),* $(,)? },
        { $( { $alt_ty:ty, $alt_variant:pat, $builder_ty:ty,  $array_ty2:ty } ),* $(,)? },
        { $( { $alt_ty2:ty, $alt_variant2:pat,$builder_ty2:ty,  $array_ty3:ty } ),* }
    ) => {
        impl ArrowArrays for DynRecordImmutableArrays {
            type Record = DynRecord;

            type Builder = DynRecordBuilder;

            fn builder(schema: Arc<ArrowSchema>, capacity: usize) -> Self::Builder {
                let mut builders: Vec<Box<dyn ArrayBuilder + Send + Sync>> = vec![];
                let mut datatypes = vec![];
                for field in schema.fields().iter().skip(2) {
                    let datatype = DataType::from(field.data_type());
                    match &datatype {
                        $(
                            $primitive_pat => {
                                builders.push(Box::new(PrimitiveBuilder::<$arrow_ty>::with_capacity(
                                    capacity,
                                )));
                            }
                        )*
                        DataType::Boolean => {
                            builders.push(Box::new(BooleanBuilder::with_capacity(capacity)));
                        }
                        $(
                            $alt_variant => {
                                builders.push(Box::new(<$builder_ty>::with_capacity(capacity, 0)));
                            }
                        )*
                        $(
                            $alt_variant2 => {
                                builders.push(Box::new(<$builder_ty2>::with_capacity(capacity)));
                            }
                        )*
                        DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                    }
                    datatypes.push(datatype);
                }
                DynRecordBuilder {
                    builders,
                    datatypes,
                    _null: arrow::array::BooleanBufferBuilder::new(capacity),
                    _ts: arrow::array::UInt32Builder::with_capacity(capacity),
                    schema: schema.clone(),
                }
            }

            fn get(
                &self,
                offset: u32,
                projection_mask: &parquet::arrow::ProjectionMask,
            ) -> Option<Option<<Self::Record as Record>::Ref<'_>>> {
                let offset = offset as usize;

                if offset >= Array::len(self._null.as_ref()) {
                    return None;
                }
                if self._null.value(offset) {
                    return Some(None);
                }

                let schema = self.record_batch.schema();
                let metadata = schema.metadata();
                let primary_key_index = metadata
                    .get("primary_key_index")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut columns = vec![];
                for (idx, arr) in self.columns.iter().enumerate() {
                    if projection_mask.leaf_included(idx + USER_COLUMN_OFFSET) {
                        let datatype = DataType::from(arr.data_type());
                        let value: Arc<dyn Value> = match &datatype {
                            $(
                                $primitive_pat => {
                                    let v = Self::primitive_value::<$arrow_ty>(arr, offset);
                                    if primary_key_index == idx {
                                        Arc::new(<$primitive_tonbo_ty>::from(v))
                                    } else {
                                        Arc::new(Some(<$primitive_tonbo_ty>::from(v)))
                                    }
                                }
                            )*
                            DataType::Boolean => {
                                let v = arr.as_any().downcast_ref::<BooleanArray>().unwrap().value(offset);
                                if primary_key_index == idx {
                                    Arc::new(v)
                                } else {
                                    Arc::new(Some(v))
                                }
                            }
                            $(
                                $alt_variant => {

                                let v = arr.as_any()
                                    .downcast_ref::<$array_ty2>()
                                    .unwrap()
                                    .value(offset)
                                    .to_owned();
                                    if primary_key_index == idx {
                                        Arc::new(v)
                                    } else {
                                        Arc::new(Some(v))
                                    }
                                }
                            )*
                            $(
                                $alt_variant2 => {
                                    let v = arr.as_any().downcast_ref::<$array_ty3>().unwrap().value(offset);
                                    if primary_key_index == idx {
                                        Arc::new(v)
                                    } else {
                                        Arc::new(Some(v))
                                    }
                                }
                            )*
                            DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                        };

                        columns.push(value);
                    } else {
                        columns.push(self.null_value(idx + 2));
                    }
                }
                Some(Some(DynRecordRef::new(columns, primary_key_index)))
            }

            fn as_record_batch(&self) -> &arrow::array::RecordBatch {
                &self.record_batch
            }
        }
    };
}

macro_rules! implement_builder_array {
    (
        { $( { $primitive_ty:ty, $primitive_pat:pat, $arrow_ty:ty } ),* $(,)? },
        { $( { $alt_ty:ty, $alt_variant:pat, $builder_ty:ty,  $array_ty2:ty } ),* $(,)? },
        { $( { $alt_ty2:ty, $alt_variant2:pat,$builder_ty2:ty, $array_ty3:ty, $value_fn:ident } ),* }
    ) => {
        impl Builder<DynRecordImmutableArrays> for DynRecordBuilder {
            fn push(
                &mut self,
                key: Ts<PrimaryKey>,
                row: Option<DynRecordRef>,
            ) {
                self._null.append(row.is_none());
                self._ts.append_value(key.ts.into());
                let schema = &self.schema;
                let metadata = schema.metadata();
                let primary_key_index = metadata
                    .get("primary_key_index")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();

                self.push_primary_key(key, primary_key_index);
                match row {
                    Some(record_ref) => {
                        for (idx, (builder, col)) in self
                            .builders
                            .iter_mut()
                            .zip(record_ref.columns.iter())
                            .enumerate()
                        {
                            if idx == primary_key_index {
                                continue;
                            }

                            let is_nullable = self.schema.field(idx + 2).is_nullable();
                            let datatype = col.data_type();
                            match datatype {
                                $(
                                    $primitive_pat => {
                                        let bd = Self::as_builder_mut::<PrimitiveBuilder<$arrow_ty>>(
                                            builder.as_mut(),
                                        );
                                        match col.as_any().downcast_ref::<Option<$primitive_ty>>().unwrap() {
                                            Some(value) => bd.append_value(*value),
                                            None if is_nullable => bd.append_null(),
                                            None => bd.append_value(Default::default()),
                                        }
                                    }
                                )*
                                DataType::Boolean => {
                                    let bd = Self::as_builder_mut::<BooleanBuilder>(builder.as_mut());
                                    match col.as_any().downcast_ref::<Option<bool>>().unwrap() {
                                        Some(value) => bd.append_value(*value),
                                        None if is_nullable => bd.append_null(),
                                        None => bd.append_value(Default::default()),
                                    }
                                }
                                $(
                                    $alt_variant => {
                                        let bd = Self::as_builder_mut::<$builder_ty>(builder.as_mut());
                                        match col.as_any().downcast_ref::<Option<$alt_ty>>().unwrap() {
                                            Some(value) => bd.append_value(value),
                                            None if is_nullable => bd.append_null(),
                                            None => bd.append_value(<$alt_ty>::default()),
                                        }
                                    }
                                )*
                                $(
                                    $alt_variant2 => {
                                        let bd = Self::as_builder_mut::<$builder_ty2>(builder.as_mut());
                                        match col.as_any().downcast_ref::<Option<$alt_ty2>>().unwrap() {
                                            Some(value) => bd.append_value((value.$value_fn())),
                                            None if is_nullable => bd.append_null(),
                                            None => bd.append_value(Default::default()),
                                        }
                                    }
                                )*
                                DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                            }
                        }
                    }
                    None => {
                        for (idx, (builder, datatype)) in self
                            .builders
                            .iter_mut()
                            .zip(self.datatypes.iter_mut())
                            .enumerate()
                        {
                            if idx == primary_key_index {
                                continue;
                            }
                            match datatype {
                                $(
                                    $primitive_pat => {
                                        Self::as_builder_mut::<PrimitiveBuilder<$arrow_ty>>(builder.as_mut())
                                            .append_value(<$primitive_ty>::default());
                                    }
                                )*
                                DataType::Boolean => {
                                    Self::as_builder_mut::<BooleanBuilder>(builder.as_mut())
                                        .append_value(bool::default());
                                }
                                $(
                                    $alt_variant => {
                                        Self::as_builder_mut::<$builder_ty>(builder.as_mut())
                                            .append_value(<$alt_ty>::default());
                                    }
                                )*
                                $(
                                    $alt_variant2 => {
                                        Self::as_builder_mut::<$builder_ty2>(builder.as_mut())
                                            .append_value(Default::default());
                                    }
                                )*
                                DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                            }
                        }
                    }
                }
            }

            fn written_size(&self) -> usize {
                let size = self._null.as_slice().len() + mem::size_of_val(self._ts.values_slice());
                self.builders
                    .iter()
                    .zip(self.datatypes.iter())
                    .fold(size, |acc, (builder, datatype)| {
                        acc + match datatype {
                            $(
                                $primitive_pat => mem::size_of_val(
                                    Self::as_builder::<PrimitiveBuilder<$arrow_ty>>(builder.as_ref())
                                        .values_slice(),
                                ),

                            )*
                            DataType::Boolean => mem::size_of_val(
                                Self::as_builder::<BooleanBuilder>(builder.as_ref()).values_slice(),
                            ),
                            $(
                                $alt_variant => mem::size_of_val(
                                    Self::as_builder::<$builder_ty>(builder.as_ref()).values_slice(),
                                ),
                            )*
                            $(
                                $alt_variant2 => mem::size_of_val(
                                    Self::as_builder::<$builder_ty2>(builder.as_ref())
                                        .values_slice()
                                ),
                            )*
                            DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                        }
                    })
            }

            fn finish(&mut self, indices: Option<&[usize]>) -> DynRecordImmutableArrays {
                let _null = Arc::new(BooleanArray::new(self._null.finish(), None));
                let _ts = Arc::new(self._ts.finish());

                let mut array_refs = vec![Arc::clone(&_null) as ArrayRef, Arc::clone(&_ts) as ArrayRef];
                for (builder, datatype) in self
                    .builders
                    .iter_mut()
                    .zip(self.datatypes.iter())
                {
                    match datatype {
                        $(
                            $primitive_pat => {
                                let value = Arc::new(
                                    Self::as_builder_mut::<PrimitiveBuilder<$arrow_ty>>(builder.as_mut())
                                        .finish(),
                                );
                                array_refs.push(value);
                            }

                        )*
                        DataType::Boolean => {
                            let value =
                                Arc::new(Self::as_builder_mut::<BooleanBuilder>(builder.as_mut()).finish());
                            array_refs.push(value);
                        }
                        $(
                            $alt_variant => {
                                let value =
                                    Arc::new(Self::as_builder_mut::<$builder_ty>(builder.as_mut()).finish());
                                array_refs.push(value);
                            }
                        )*
                        $(
                            $alt_variant2 => {
                                let value = Arc::new(
                                    Self::as_builder_mut::<$builder_ty2>(builder.as_mut())
                                        .finish(),
                                );
                                array_refs.push(value.clone());
                            }
                        )*
                        DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                    };
                }

                let columns = array_refs[2..].to_vec();
                let mut record_batch =
                    arrow::record_batch::RecordBatch::try_new(self.schema.clone(), array_refs.clone())
                        .expect("create record batch must be successful");
                if let Some(indices) = indices {
                    record_batch = record_batch
                        .project(indices)
                        .expect("projection indices must be successful");
                }

                DynRecordImmutableArrays {
                    _null,
                    _ts,
                    columns,
                    record_batch,
                }
            }
        }

        impl DynRecordBuilder {
            fn push_primary_key(
                &mut self,
                key: Ts<PrimaryKey>,
                primary_key_index: usize,
            ) {
                let builder = self.builders.get_mut(primary_key_index).unwrap();
                let datatype = self.datatypes.get_mut(primary_key_index).unwrap();
                let col = key.value;
                let value = col.get(0).unwrap();
                match datatype {
                    $(
                        $primitive_pat => {
                            let value = col.get(0).unwrap().as_any().downcast_ref::<$primitive_ty>().unwrap();
                            Self::as_builder_mut::<PrimitiveBuilder<$arrow_ty>>(builder.as_mut())
                                .append_value(*value.as_any().downcast_ref::<$primitive_ty>().unwrap())
                        }
                    )*
                    DataType::Boolean => Self::as_builder_mut::<BooleanBuilder>(builder.as_mut())
                        .append_value(*value.as_any().downcast_ref::<bool>().unwrap()),
                    $(
                        $alt_variant => {
                            Self::as_builder_mut::<$builder_ty>(builder.as_mut())
                                .append_value(value.as_any().downcast_ref::<$alt_ty>().unwrap())
                        }
                    )*
                    $(
                        $alt_variant2 => {
                            Self::as_builder_mut::<$builder_ty2>(builder.as_mut())
                                .append_value(value.as_any().downcast_ref::<$alt_ty2>().unwrap().$value_fn())
                        }
                    )*
                    DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                }
            }
        }
    };
}

implement_arrow_array!(
    {
        // primitive_ty type
        { u8, u8, DataType::UInt8, UInt8Type },
        { u16, u16, DataType::UInt16, UInt16Type },
        { u32, u32, DataType::UInt32, UInt32Type },
        { u64, u64, DataType::UInt64, UInt64Type },
        { i8, i8, DataType::Int8, Int8Type },
        { i16, i16, DataType::Int16, Int16Type },
        { i32, i32, DataType::Int32, Int32Type },
        { i64, i64, DataType::Int64, Int64Type },
        { f32, F32, DataType::Float32, Float32Type },
        { f64, F64, DataType::Float64, Float64Type },
        { i32, Date32, DataType::Date32, Date32Type },
        { i64, Date64, DataType::Date64, Date64Type },
    },
    // f32, f64, and bool are special cases, they are handled separately
    {
        { String, DataType::String, StringBuilder, StringArray },
        { LargeString, DataType::LargeString, LargeStringBuilder, LargeStringArray },
        { Vec<u8>, DataType::Bytes, GenericBinaryBuilder<i32>, GenericBinaryArray<i32> },
        { LargeBinary, DataType::LargeBinary, GenericBinaryBuilder<i64>, GenericBinaryArray<i64> }
    },
    {
        { Timestamp, DataType::Timestamp(TimeUnit::Second), TimestampSecondBuilder, TimestampSecondArray },
        { Timestamp, DataType::Timestamp(TimeUnit::Millisecond), TimestampMillisecondBuilder,  TimestampMillisecondArray },
        { Timestamp, DataType::Timestamp(TimeUnit::Microsecond),TimestampMicrosecondBuilder, TimestampMicrosecondArray },
        { Timestamp, DataType::Timestamp(TimeUnit::Nanosecond),TimestampNanosecondBuilder, TimestampNanosecondArray },
        { Time32, DataType::Time32(TimeUnit::Second), Time32SecondBuilder, Time32SecondArray },
        { Time32, DataType::Time32(TimeUnit::Millisecond), Time32MillisecondBuilder,  Time32MillisecondArray },
        { Time64, DataType::Time64(TimeUnit::Microsecond),Time64MicrosecondBuilder, Time64MicrosecondArray },
        { Time64, DataType::Time64(TimeUnit::Nanosecond),Time64NanosecondBuilder, Time64NanosecondArray }
    }
);

implement_builder_array!(
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
    {

        { String, DataType::String, StringBuilder, StringArray },
        { LargeString, DataType::LargeString, LargeStringBuilder, LargeStringArray },
        { Vec<u8>, DataType::Bytes, GenericBinaryBuilder<i32>, GenericBinaryArray<i32> },
        { LargeBinary, DataType::LargeBinary, GenericBinaryBuilder<i64>, GenericBinaryArray<i64> }
    },
    {
        { F32, DataType::Float32, Float32Builder, Float32Array, value },
        { F64, DataType::Float64, Float64Builder, Float64Array, value },
        { Date32, DataType::Date32, Date32Builder, Date32Array, value },
        { Date64, DataType::Date64, Date64Builder, Date64Array, value },
        { Timestamp, DataType::Timestamp(TimeUnit::Second), TimestampSecondBuilder, TimestampSecondArray, timestamp },
        { Timestamp, DataType::Timestamp(TimeUnit::Millisecond), TimestampMillisecondBuilder,  TimestampMillisecondArray, timestamp_millis },
        { Timestamp, DataType::Timestamp(TimeUnit::Microsecond),TimestampMicrosecondBuilder, TimestampMicrosecondArray, timestamp_micros },
        { Timestamp, DataType::Timestamp(TimeUnit::Nanosecond),TimestampNanosecondBuilder, TimestampNanosecondArray, timestamp_nanos },
        { Time32, DataType::Time32(TimeUnit::Second), Time32SecondBuilder, Time32SecondArray, value },
        { Time32, DataType::Time32(TimeUnit::Millisecond), Time32MillisecondBuilder,  Time32MillisecondArray, value },
        { Time64, DataType::Time64(TimeUnit::Microsecond),Time64MicrosecondBuilder, Time64MicrosecondArray, value },
        { Time64, DataType::Time64(TimeUnit::Nanosecond),Time64NanosecondBuilder, Time64NanosecondArray, value }
    }
);

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use common::{F32, F64};
    use parquet::arrow::ProjectionMask;

    use crate::{
        dyn_record,
        inmem::immutable::{ArrowArrays, Builder},
        record::{DynRecordImmutableArrays, DynRecordRef, Record, RecordRef, Schema},
    };

    #[tokio::test]
    async fn test_build_primary_key() {
        {
            let schema = Arc::new(Schema::new(
                vec![Field::new("id", DataType::UInt64, false)],
                0,
            ));
            let record = dyn_record!(("id", UInt64, false, Arc::new(1_u64)), 0);
            let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
            let key = crate::timestamp::Ts {
                ts: 0.into(),
                value: record.as_record_ref().key(),
            };
            builder.push(key.clone(), Some(record.as_record_ref()));
            builder.push(key.clone(), None);
            builder.push(key.clone(), Some(record.as_record_ref()));
            let arrays = builder.finish(None);
            let res = arrays.get(0, &ProjectionMask::all());
            let cols = res.unwrap().unwrap().columns;
            assert_eq!(cols.len(), 1);
            for (actual, expected) in cols.iter().zip(record.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
            }
        }
        {
            let schema = Arc::new(Schema::new(
                vec![Field::new("id", DataType::Utf8, false)],
                0,
            ));
            let record = dyn_record!(("id", String, false, Arc::new("abc".to_string())), 0);
            let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
            let key = crate::timestamp::Ts {
                ts: 0.into(),
                value: record.as_record_ref().key(),
            };
            builder.push(key.clone(), Some(record.as_record_ref()));
            builder.push(key.clone(), None);
            builder.push(key.clone(), Some(record.as_record_ref()));
            let arrays = builder.finish(None);
            let res = arrays.get(0, &ProjectionMask::all());
            let cols = res.unwrap().unwrap().columns;
            assert_eq!(cols.len(), 1);
            for (actual, expected) in cols.iter().zip(record.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
            }
        }
        {
            let schema = Arc::new(Schema::new(
                vec![Field::new("id", DataType::Float32, false)],
                0,
            ));
            let record = dyn_record!(("id", Float32, false, Arc::new(F32::from(3.2324))), 0);
            let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
            let key = crate::timestamp::Ts {
                ts: 0.into(),
                value: record.as_record_ref().key(),
            };
            builder.push(key.clone(), Some(record.as_record_ref()));
            builder.push(key.clone(), None);
            builder.push(key.clone(), Some(record.as_record_ref()));
            let arrays = builder.finish(None);
            let res = arrays.get(0, &ProjectionMask::all());
            let cols = res.unwrap().unwrap().columns;
            assert_eq!(cols.len(), 1);
            for (actual, expected) in cols.iter().zip(record.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
            }
        }
    }

    #[tokio::test]
    async fn test_build_array() {
        let schema = Arc::new(Schema::new(
            vec![
                Field::new("id", DataType::UInt32, false),
                Field::new("bool", DataType::Boolean, true),
                Field::new("bytes", DataType::Binary, true),
                Field::new("none", DataType::Int64, true),
                Field::new("str", DataType::Utf8, false),
                Field::new("float32", DataType::Float32, false),
                Field::new("float64", DataType::Float64, true),
            ],
            0,
        ));
        let record = dyn_record!(
            ("id", UInt32, false, Arc::new(1_u32)),
            ("bool", Boolean, true, Arc::new(Some(true))),
            ("bytes", Bytes, true, Arc::new(Some(vec![1_u8, 2, 3, 4]))),
            ("none", Int64, true, Arc::new(None::<i64>)),
            ("str", String, false, Arc::new("tonbo".to_string())),
            ("float32", Float32, false, Arc::new(F32::from(1.09_f32))),
            (
                "float64",
                Float64,
                true,
                Arc::new(Some(F64::from(3.09_f64)))
            ),
            0
        );

        let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
        let key = crate::timestamp::Ts {
            ts: 0.into(),
            value: record.as_record_ref().key(),
        };
        builder.push(key.clone(), Some(record.as_record_ref()));
        builder.push(key.clone(), None);
        builder.push(key.clone(), Some(record.as_record_ref()));
        let arrays = builder.finish(None);

        {
            let res = arrays.get(0, &ProjectionMask::all());
            let cols = res.unwrap().unwrap().columns;
            for (i, (actual, expected)) in cols
                .iter()
                .zip(record.as_record_ref().columns.iter())
                .enumerate()
            {
                // TODO: set value to None instead of default value when pushing
                if i != 3 {
                    assert_eq!(actual, expected);
                }
            }
        }
        {
            let record_batch = arrays.as_record_batch();
            let mask = ProjectionMask::all();
            let record_ref =
                DynRecordRef::from_record_batch(record_batch, 0, &mask, schema.arrow_schema());
            assert_eq!(
                record_ref.get().unwrap().columns,
                record.as_record_ref().columns
            );
        }
    }
}
