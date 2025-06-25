use std::{any::Any, mem, sync::Arc};

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
        Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        Int8Type, Schema as ArrowSchema, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};

use super::{record::DynRecord, record_ref::DynRecordRef, value::Value, DataType};
use crate::{
    cast_arc_value,
    inmem::immutable::{ArrowArrays, Builder},
    magic::USER_COLUMN_OFFSET,
    record::{
        Date32, Date64, Key, LargeBinary, LargeString, Record, Schema, Time32, Time64, TimeUnit,
        Timestamp, F32, F64,
    },
    timestamp::Ts,
};

#[allow(unused)]
pub struct DynRecordImmutableArrays {
    _null: Arc<arrow::array::BooleanArray>,
    _ts: Arc<arrow::array::UInt32Array>,
    columns: Vec<Value>,
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
    fn primitive_value<T>(col: &Value, offset: usize) -> T::Native
    where
        T: ArrowPrimitiveType,
    {
        cast_arc_value!(col.value, PrimitiveArray<T>).value(offset)
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
                for (idx, col) in self.columns.iter().enumerate() {
                    if projection_mask.leaf_included(idx + USER_COLUMN_OFFSET) {
                        let datatype = col.datatype();
                        let name = col.desc.name.to_string();
                        let nullable = col.is_nullable();
                        let value: Arc<dyn Any + Send + Sync> = match &datatype {
                            $(
                                $primitive_pat => {
                                    let v = Self::primitive_value::<$arrow_ty>(col, offset);
                                    if primary_key_index == idx {
                                        Arc::new(<$primitive_tonbo_ty>::from(v))
                                    } else {
                                        Arc::new(Some(<$primitive_tonbo_ty>::from(v)))
                                    }
                                }
                            )*
                            DataType::Boolean => {
                                let v = cast_arc_value!(col.value, BooleanArray).value(offset);
                                if primary_key_index == idx {
                                    Arc::new(v)
                                } else {
                                    Arc::new(Some(v))
                                }
                            }
                            $(
                                $alt_variant => {
                                let v = cast_arc_value!(col.value, $array_ty2)
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
                                    let v = cast_arc_value!(col.value, $array_ty3).value(offset);
                                    if primary_key_index == idx {
                                        Arc::new(v)
                                    } else {
                                        Arc::new(Some(v))
                                    }
                                }
                            )*
                            DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                        };

                        columns.push(Value::new(datatype, name, value, nullable));
                    } else {
                        columns.push(col.clone());
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
                key: Ts<<<<DynRecord as Record>::Schema as Schema>::Key as Key>::Ref<'_>>,
                row: Option<DynRecordRef>,
            ) {
                self._null.append(row.is_none());
                self._ts.append_value(key.ts.into());
                let metadata = self.schema.metadata();
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
                            let datatype = col.datatype();
                            match datatype {
                                $(
                                    $primitive_pat => {
                                        let bd = Self::as_builder_mut::<PrimitiveBuilder<$arrow_ty>>(
                                            builder.as_mut(),
                                        );
                                        match cast_arc_value!(col.value, Option<$primitive_ty>) {
                                            Some(value) => bd.append_value(*value),
                                            None if col.is_nullable() => bd.append_null(),
                                            None => bd.append_value(Default::default()),
                                        }
                                    }
                                )*
                                DataType::Boolean => {
                                    let bd = Self::as_builder_mut::<BooleanBuilder>(builder.as_mut());
                                    match cast_arc_value!(col.value, Option<bool>) {
                                        Some(value) => bd.append_value(*value),
                                        None if col.is_nullable() => bd.append_null(),
                                        None => bd.append_value(Default::default()),
                                    }
                                }
                                $(
                                    $alt_variant => {
                                        let bd = Self::as_builder_mut::<$builder_ty>(builder.as_mut());
                                        match cast_arc_value!(col.value, Option<$alt_ty>) {
                                            Some(value) => bd.append_value(value),
                                            None if col.is_nullable() => bd.append_null(),
                                            None => bd.append_value(<$alt_ty>::default()),
                                        }
                                    }
                                )*
                                $(
                                    $alt_variant2 => {
                                        let bd = Self::as_builder_mut::<$builder_ty2>(builder.as_mut());
                                        match cast_arc_value!(col.value, Option<$alt_ty2>) {
                                            Some(value) => bd.append_value((value.$value_fn())),
                                            None if col.is_nullable() => bd.append_null(),
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
                let mut columns = vec![];
                let _null = Arc::new(BooleanArray::new(self._null.finish(), None));
                let _ts = Arc::new(self._ts.finish());

                let mut array_refs = vec![Arc::clone(&_null) as ArrayRef, Arc::clone(&_ts) as ArrayRef];
                for (idx, (builder, datatype)) in self
                    .builders
                    .iter_mut()
                    .zip(self.datatypes.iter())
                    .enumerate()
                {
                    let field = self.schema.field(idx + USER_COLUMN_OFFSET);
                    let is_nullable = field.is_nullable();
                    match datatype {
                        $(
                            $primitive_pat => {
                                let value = Arc::new(
                                    Self::as_builder_mut::<PrimitiveBuilder<$arrow_ty>>(builder.as_mut())
                                        .finish(),
                                );
                                columns.push(Value::new(
                                    *datatype,
                                    field.name().to_owned(),
                                    value.clone(),
                                    is_nullable,
                                ));
                                array_refs.push(value);
                            }

                        )*
                        DataType::Boolean => {
                            let value =
                                Arc::new(Self::as_builder_mut::<BooleanBuilder>(builder.as_mut()).finish());
                            columns.push(Value::new(
                                DataType::Boolean,
                                field.name().to_owned(),
                                value.clone(),
                                is_nullable,
                            ));
                            array_refs.push(value);
                        }
                        $(
                            $alt_variant => {
                                let value =
                                    Arc::new(Self::as_builder_mut::<$builder_ty>(builder.as_mut()).finish());
                                columns.push(Value::new(
                                    *datatype,
                                    field.name().to_owned(),
                                    value.clone(),
                                    is_nullable,
                                ));
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
                                columns.push(Value::new(
                                    *datatype,
                                    field.name().to_owned(),
                                    value,
                                    is_nullable,
                                ));
                            }
                        )*
                        DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                    };
                }

                let mut record_batch =
                    arrow::record_batch::RecordBatch::try_new(self.schema.clone(), array_refs)
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
                key: Ts<<<<DynRecord as Record>::Schema as Schema>::Key as Key>::Ref<'_>>,
                primary_key_index: usize,
            ) {
                let builder = self.builders.get_mut(primary_key_index).unwrap();
                let datatype = self.datatypes.get_mut(primary_key_index).unwrap();
                let col = key.value;
                match datatype {
                    $(
                        $primitive_pat => {
                            Self::as_builder_mut::<PrimitiveBuilder<$arrow_ty>>(builder.as_mut())
                                .append_value(*cast_arc_value!(col.value, $primitive_ty))
                        }
                    )*
                    DataType::Boolean => Self::as_builder_mut::<BooleanBuilder>(builder.as_mut())
                        .append_value(*cast_arc_value!(col.value, bool)),
                    $(
                        $alt_variant => {
                            Self::as_builder_mut::<$builder_ty>(builder.as_mut())
                                .append_value(cast_arc_value!(col.value, $alt_ty))
                        }
                    )*
                    $(
                        $alt_variant2 => {
                            Self::as_builder_mut::<$builder_ty2>(builder.as_mut())
                                .append_value(cast_arc_value!(col.value, $alt_ty2).$value_fn())
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
        // { f32, DataType::Float32, PrimitiveBuilder<Float32Type> },
        // { f64, DataType::Float64, PrimitiveBuilder<Float64Type> },
    },
    // f32, f64, and bool are special cases, they are handled separately
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

    use parquet::arrow::ProjectionMask;

    use crate::{
        dyn_record, dyn_schema,
        inmem::immutable::{ArrowArrays, Builder},
        record::{DynRecordImmutableArrays, DynRecordRef, Record, RecordRef, Schema, F32, F64},
    };

    #[tokio::test]
    async fn test_build_primary_key() {
        {
            let schema = dyn_schema!(("id", UInt64, false), 0);
            let record = dyn_record!(("id", UInt64, false, 1_u64), 0);
            let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
            let key = crate::timestamp::Ts {
                ts: 0.into(),
                value: record.key(),
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
            let schema = dyn_schema!(("id", String, false), 0);
            let record = dyn_record!(("id", String, false, "abc".to_string()), 0);
            let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
            let key = crate::timestamp::Ts {
                ts: 0.into(),
                value: record.key(),
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
            let schema = dyn_schema!(("id", Float32, false), 0);
            let record = dyn_record!(("id", Float32, false, F32::from(3.2324)), 0);
            let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
            let key = crate::timestamp::Ts {
                ts: 0.into(),
                value: record.key(),
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
        let schema = dyn_schema!(
            ("id", UInt32, false),
            ("bool", Boolean, true),
            ("bytes", Bytes, true),
            ("none", Int64, true),
            ("str", String, false),
            ("float32", Float32, false),
            ("float64", Float64, true),
            0
        );

        let record = dyn_record!(
            ("id", UInt32, false, 1_u32),
            ("bool", Boolean, true, Some(true)),
            ("bytes", Bytes, true, Some(vec![1_u8, 2, 3, 4])),
            ("none", Int64, true, None::<i64>),
            ("str", String, false, "tonbo".to_string()),
            ("float32", Float32, false, F32::from(1.09_f32)),
            ("float64", Float64, true, Some(F64::from(3.09_f64))),
            0
        );

        let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
        let key = crate::timestamp::Ts {
            ts: 0.into(),
            value: record.key(),
        };
        builder.push(key.clone(), Some(record.as_record_ref()));
        builder.push(key.clone(), None);
        builder.push(key.clone(), Some(record.as_record_ref()));
        let arrays = builder.finish(None);

        {
            let res = arrays.get(0, &ProjectionMask::all());
            let cols = res.unwrap().unwrap().columns;
            for (actual, expected) in cols.iter().zip(record.as_record_ref().columns.iter()) {
                // TODO: set value to None instead of default value when pushing
                if actual.name() != "none" {
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
