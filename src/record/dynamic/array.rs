use std::{mem, sync::Arc};

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBufferBuilder, BooleanBuilder,
        Date32Builder, Date64Builder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder,
        GenericBinaryBuilder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
        LargeStringBuilder, PrimitiveBuilder, StringBuilder, Time32MillisecondBuilder,
        Time32SecondBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        TimestampSecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    },
    datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema as ArrowSchema,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};

use super::{record::DynRecord, record_ref::DynRecordRef, AsValue, DataType};
use crate::{
    magic::USER_COLUMN_OFFSET,
    record::{
        builder::NestedBuilder, ArrowArrays, ArrowArraysBuilder, Key, LargeBinary, LargeString,
        Record, Schema, TimeUnit, ValueRef,
    },
    version::timestamp::Ts,
};

#[allow(unused)]
pub struct DynRecordImmutableArrays {
    _null: Arc<arrow::array::BooleanArray>,
    _ts: Arc<arrow::array::UInt32Array>,
    arrays: Vec<ArrayRef>,
    record_batch: arrow::record_batch::RecordBatch,
}

pub struct DynRecordBuilder {
    builders: Vec<Box<dyn ArrayBuilder + Send + Sync>>,
    datatypes: Vec<DataType>,
    _null: BooleanBufferBuilder,
    _ts: UInt32Builder,
    schema: Arc<ArrowSchema>,
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
        { $( { $primitive_pat:pat, $arrow_ty:ty } ),* $(,)? },
        { $( { $alt_variant:pat, $builder_ty:ty } ),*  },
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
                                builders.push(Box::new(<$arrow_ty>::with_capacity(
                                    capacity,
                                )));
                            }
                        )*
                        $(
                            $alt_variant => {
                                builders.push(Box::new(<$builder_ty>::with_capacity(capacity, 0)));
                            }
                        )*
                        DataType::FixedSizeBinary(w) => builders.push(Box::new(FixedSizeBinaryBuilder::with_capacity(capacity, *w))),
                        DataType::List(_) | DataType::Dictionary(_, _) => builders.push(Box::new(NestedBuilder::with_capacity(field.clone(), capacity))),
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
                for (idx, array) in self.arrays.iter().enumerate() {
                    if projection_mask.leaf_included(idx + USER_COLUMN_OFFSET) {
                        let value_ref = ValueRef::from_array_ref(array, offset).unwrap();
                        columns.push(value_ref);
                    } else {
                        columns.push(ValueRef::Null);
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
        { $( { $primitive_ty:ty, $primitive_pat:pat, $arrow_ty:ty, $as_primitive_value:ident } ),* $(,)? },
        { $( { $alt_ty:ty, $alt_variant:pat, $builder_ty:ty, $as_alt_value:ident } ),* $(,)? },
        { $( { $alt_ty2:ty, $alt_variant2:pat,$builder_ty2:ty, $array_ty3:ty, $as_alt_value2:ident } ),* }
    ) => {
        impl ArrowArraysBuilder<DynRecordImmutableArrays> for DynRecordBuilder {
            fn push(
                &mut self,
                key: Ts<<<<DynRecord as Record>::Schema as Schema>::Key as Key>::Ref<'_>>,
                row: Option<DynRecordRef>,
            ) {
                self._null.append(row.is_none());
                self._ts.append_value(key.ts.into());
                match row {
                    Some(record_ref) => {
                        for (idx, (builder, col)) in self
                            .builders
                            .iter_mut()
                            .zip(record_ref.columns.iter())
                            .enumerate()
                        {
                            let field = self.schema.field(idx + USER_COLUMN_OFFSET);
                            let is_nullable = field.is_nullable();
                            let datatype = DataType::from(field.data_type());
                            match datatype {
                                $(
                                    $primitive_pat => {
                                        let bd = Self::as_builder_mut::<PrimitiveBuilder<$arrow_ty>>(
                                            builder.as_mut(),
                                        );
                                        match col.$as_primitive_value() {
                                            Some(value) => bd.append_value(*value),
                                            None if is_nullable => bd.append_null(),
                                            None => bd.append_value(Default::default()),
                                        }
                                    }
                                )*
                                DataType::Boolean => {
                                    let bd = Self::as_builder_mut::<BooleanBuilder>(builder.as_mut());
                                    match col.as_bool_opt() {
                                        Some(value) => bd.append_value(*value),
                                        None if is_nullable => bd.append_null(),
                                        None => bd.append_value(Default::default()),
                                    }
                                }
                                $(
                                    $alt_variant => {
                                        let bd = Self::as_builder_mut::<$builder_ty>(builder.as_mut());
                                        match col.$as_alt_value() {
                                            Some(value) => bd.append_value(value),
                                            None if is_nullable => bd.append_null(),
                                            None => bd.append_value(<$alt_ty>::default()),
                                        }
                                    }
                                )*
                                $(
                                    $alt_variant2 => {
                                        let bd = Self::as_builder_mut::<$builder_ty2>(builder.as_mut());
                                        match col.$as_alt_value2() {
                                            Some(value) => bd.append_value(*value),
                                            None if is_nullable => bd.append_null(),
                                            None => bd.append_value(Default::default()),
                                        }
                                    }
                                )*
                                DataType::FixedSizeBinary(w) =>{
                                    let bd = Self::as_builder_mut::<FixedSizeBinaryBuilder>(builder.as_mut());
                                    match col.as_bytes_opt() {
                                        Some(value) => bd.append_value(value).unwrap(),
                                        None if is_nullable => bd.append_null(),
                                        None => bd.append_value(vec![0; w as usize]).unwrap(),
                                    }
                                }
                                DataType::List(_) | DataType::Dictionary(_, _) =>{
                                    let bd = Self::as_builder_mut::<NestedBuilder>(builder.as_mut());
                                    // TODO: remove this clone
                                    bd.append_value(col.clone())
                                }
                                DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                            }
                        }
                    }
                    None => {
                        // For tombstones (row == None), ensure the primary key column is still
                        // populated from the provided key, so ordering and lookups remain correct.
                        let primary_key_index = self
                            .schema
                            .metadata()
                            .get("primary_key_index")
                            .expect("primary key index must exist in schema metadata")
                            .parse::<usize>()
                            .expect("primary key index must be a valid usize");

                        for (idx, (builder, datatype)) in self
                            .builders
                            .iter_mut()
                            .zip(self.datatypes.iter_mut())
                            .enumerate()
                        {
                            let field = self.schema.field(idx + USER_COLUMN_OFFSET);
                            let is_nullable = field.is_nullable();

                            if idx == primary_key_index {
                                match datatype {
                                    $(
                                        $primitive_pat => {
                                            let bd = Self::as_builder_mut::<PrimitiveBuilder<$arrow_ty>>(builder.as_mut());
                                            match key.value.$as_primitive_value() {
                                                Some(value) => bd.append_value(*value),
                                                None => bd.append_value(Default::default()),
                                            }
                                        }
                                    )*
                                    DataType::Boolean => {
                                        let bd = Self::as_builder_mut::<BooleanBuilder>(builder.as_mut());
                                        match key.value.as_bool_opt() {
                                            Some(value) => bd.append_value(*value),
                                            None => bd.append_value(Default::default()),
                                        }
                                    }
                                    $(
                                        $alt_variant => {
                                            let bd = Self::as_builder_mut::<$builder_ty>(builder.as_mut());
                                            match key.value.$as_alt_value() {
                                                Some(value) => bd.append_value(value),
                                                None => bd.append_value(<$alt_ty>::default()),
                                            }
                                        }
                                    )*
                                    $(
                                        $alt_variant2 => {
                                            let bd = Self::as_builder_mut::<$builder_ty2>(builder.as_mut());
                                            match key.value.$as_alt_value2() {
                                                Some(value) => bd.append_value(*value),
                                                None => bd.append_value(Default::default()),
                                            }
                                        }
                                    )*
                                    DataType::FixedSizeBinary(w) => {
                                        let bd = Self::as_builder_mut::<FixedSizeBinaryBuilder>(builder.as_mut());
                                        match key.value.as_bytes_opt() {
                                            Some(value) => bd.append_value(value).unwrap(),
                                            None => bd.append_value(vec![0; *w as usize]).unwrap(),
                                        }
                                    }
                                    DataType::List(_) | DataType::Dictionary(_, _)=> {
                                        // Lists are not allowed as primary keys; append default
                                        let bd = Self::as_builder_mut::<NestedBuilder>(builder.as_mut());
                                        bd.append_default();
                                    }
                                    DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                                }
                            } else {
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
                                    DataType::FixedSizeBinary(w) =>{
                                        Self::as_builder_mut::<FixedSizeBinaryBuilder>(builder.as_mut())
                                            .append_value(vec![0; *w as usize]).unwrap();
                                    }
                                    DataType::List(_) | DataType::Dictionary(_, _) =>{
                                        let bd = Self::as_builder_mut::<NestedBuilder>(builder.as_mut());
                                        if is_nullable {
                                            bd.append_null();
                                        } else {
                                            bd.append_default();
                                        }
                                    }
                                    DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                                }
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
                            DataType::FixedSizeBinary(_) => mem::size_of_val(
                                Self::as_builder::<FixedSizeBinaryBuilder>(builder.as_ref()).values_slice()
                            ),
                            DataType::List(_) | DataType::Dictionary(_, _) => {
                                Self::as_builder::<NestedBuilder>(builder.as_ref()).bytes_written()
                            },
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
                                let array = Arc::new(
                                    Self::as_builder_mut::<PrimitiveBuilder<$arrow_ty>>(builder.as_mut())
                                        .finish(),
                                );
                                array_refs.push(array);
                            }

                        )*
                        DataType::Boolean => {
                            let array =
                                Arc::new(Self::as_builder_mut::<BooleanBuilder>(builder.as_mut()).finish());
                            array_refs.push(array);
                        }
                        $(
                            $alt_variant => {
                                let array =
                                    Arc::new(Self::as_builder_mut::<$builder_ty>(builder.as_mut()).finish());
                                array_refs.push(array);
                            }
                        )*
                        $(
                            $alt_variant2 => {
                                let array = Arc::new(
                                    Self::as_builder_mut::<$builder_ty2>(builder.as_mut())
                                        .finish(),
                                );
                                array_refs.push(array.clone());
                            }
                        )*
                        DataType::FixedSizeBinary(_) => {
                            let array = Arc::new(
                                Self::as_builder_mut::<FixedSizeBinaryBuilder>(builder.as_mut())
                                    .finish(),
                            );
                            array_refs.push(array.clone());
                        }
                        DataType::List(_) | DataType::Dictionary(_, _) => {
                            let array = Arc::new(
                                Self::as_builder_mut::<NestedBuilder>(builder.as_mut())
                                    .finish(),
                            );
                            array_refs.push(array.clone());
                        }

                        DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
                    };
                }

                let arrays = array_refs[2..].to_vec();
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
                    arrays,
                    record_batch,
                }
            }
        }

    };
}

implement_arrow_array!(
    {
        // primitive_ty type
        {  DataType::Boolean, BooleanBuilder },
        {  DataType::UInt8, UInt8Builder },
        {  DataType::UInt16, UInt16Builder },
        {  DataType::UInt32, UInt32Builder },
        {  DataType::UInt64, UInt64Builder },
        {  DataType::Int8, Int8Builder },
        {  DataType::Int16, Int16Builder },
        {  DataType::Int32, Int32Builder },
        {  DataType::Int64, Int64Builder },
        {  DataType::Float32, Float32Builder },
        {  DataType::Float64, Float64Builder },
        {  DataType::Date32, Date32Builder },
        {  DataType::Date64, Date64Builder },
        { DataType::Timestamp(TimeUnit::Second), TimestampSecondBuilder },
        { DataType::Timestamp(TimeUnit::Millisecond), TimestampMillisecondBuilder },
        { DataType::Timestamp(TimeUnit::Microsecond),TimestampMicrosecondBuilder },
        { DataType::Timestamp(TimeUnit::Nanosecond),TimestampNanosecondBuilder },
        { DataType::Time32(TimeUnit::Second), Time32SecondBuilder },
        { DataType::Time32(TimeUnit::Millisecond), Time32MillisecondBuilder },
        { DataType::Time64(TimeUnit::Microsecond),Time64MicrosecondBuilder },
        { DataType::Time64(TimeUnit::Nanosecond),Time64NanosecondBuilder }
    },
    // f32, f64, and bool are special cases, they are handled separately
    {
        { DataType::String, StringBuilder },
        { DataType::LargeString, LargeStringBuilder },
        { DataType::Bytes, GenericBinaryBuilder<i32> },
        { DataType::LargeBinary, GenericBinaryBuilder<i64> }
    },
);

implement_builder_array!(
    {
        // primitive_ty type
        { u8, DataType::UInt8, UInt8Type, as_u8_opt },
        { u16, DataType::UInt16, UInt16Type, as_u16_opt },
        { u32, DataType::UInt32, UInt32Type, as_u32_opt },
        { u64, DataType::UInt64, UInt64Type, as_u64_opt },
        { i8, DataType::Int8, Int8Type, as_i8_opt },
        { i16, DataType::Int16, Int16Type, as_i16_opt },
        { i32, DataType::Int32, Int32Type, as_i32_opt },
        { i64, DataType::Int64, Int64Type, as_i64_opt },
        { f32, DataType::Float32, Float32Type, as_f32_opt },
        { f64, DataType::Float64, Float64Type, as_f64_opt },
    },
    // String/binary types and bool are special cases, they are handled separately
    {
        { String, DataType::String, StringBuilder, as_string_opt },
        { LargeString, DataType::LargeString, LargeStringBuilder, as_string_opt },
        { Vec<u8>, DataType::Bytes, GenericBinaryBuilder<i32>, as_bytes_opt },
        { LargeBinary, DataType::LargeBinary, GenericBinaryBuilder<i64>, as_bytes_opt }
    },
    {
        { Date32, DataType::Date32, Date32Builder, Date32Array, as_i32_opt },
        { Date64, DataType::Date64, Date64Builder, Date64Array, as_i64_opt },
        { Timestamp, DataType::Timestamp(TimeUnit::Second), TimestampSecondBuilder, TimestampSecondArray, as_i64_opt },
        { Timestamp, DataType::Timestamp(TimeUnit::Millisecond), TimestampMillisecondBuilder,  TimestampMillisecondArray, as_i64_opt },
        { Timestamp, DataType::Timestamp(TimeUnit::Microsecond),TimestampMicrosecondBuilder, TimestampMicrosecondArray, as_i64_opt },
        { Timestamp, DataType::Timestamp(TimeUnit::Nanosecond),TimestampNanosecondBuilder, TimestampNanosecondArray, as_i64_opt },
        { Time32, DataType::Time32(TimeUnit::Second), Time32SecondBuilder, Time32SecondArray, as_i32_opt },
        { Time32, DataType::Time32(TimeUnit::Millisecond), Time32MillisecondBuilder,  Time32MillisecondArray, as_i32_opt },
        { Time64, DataType::Time64(TimeUnit::Microsecond),Time64MicrosecondBuilder, Time64MicrosecondArray, as_i64_opt },
        { Time64, DataType::Time64(TimeUnit::Nanosecond),Time64NanosecondBuilder, Time64NanosecondArray, as_i64_opt }
    }
);

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, TimeUnit as ArrowTimeUnit};
    use parquet::arrow::ProjectionMask;

    use crate::{
        dyn_schema,
        record::{
            ArrowArrays, ArrowArraysBuilder, DictionaryKeyType, DynRecord,
            DynRecordImmutableArrays, DynRecordRef, DynSchema, DynamicField, Record, RecordRef,
            Schema, TimeUnit, Value, ValueRef,
        },
    };

    #[test]
    fn test_build_primary_key() {
        {
            let schema = dyn_schema!(("id", UInt64, false), 0);
            let record = DynRecord::new(vec![Value::UInt64(1_u64)], 0);
            let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
            let key = crate::version::timestamp::Ts {
                ts: 0.into(),
                value: record.key(),
            };
            builder.push(key.clone(), Some(record.as_record_ref()));
            builder.push(key.clone(), None);
            builder.push(key, Some(record.as_record_ref()));
            let arrays = builder.finish(None);
            let res = arrays.get(0, &ProjectionMask::all());
            let cols = res.unwrap().unwrap().columns;
            assert_eq!(cols.len(), 1);
            for (actual, expected) in cols.iter().zip(record.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
            }
        }
        {
            let schema = dyn_schema!(("id", Utf8, false), 0);
            let record = DynRecord::new(vec![Value::String("abc".to_string())], 0);
            let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
            let key = crate::version::timestamp::Ts {
                ts: 0.into(),
                value: record.key(),
            };
            builder.push(key.clone(), Some(record.as_record_ref()));
            builder.push(key.clone(), None);
            builder.push(key, Some(record.as_record_ref()));
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
            let record = DynRecord::new(vec![Value::Float32(3.2324)], 0);
            let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
            let key = crate::version::timestamp::Ts {
                ts: 0.into(),
                value: record.key(),
            };
            builder.push(key.clone(), Some(record.as_record_ref()));
            builder.push(key.clone(), None);
            builder.push(key, Some(record.as_record_ref()));
            let arrays = builder.finish(None);
            let res = arrays.get(0, &ProjectionMask::all());
            let cols = res.unwrap().unwrap().columns;
            assert_eq!(cols.len(), 1);
            for (actual, expected) in cols.iter().zip(record.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn test_tombstone_keeps_primary_key() {
        use arrow::array::AsArray;
        // Ensure that when pushing a tombstone (row == None), the primary key column
        // is still populated from the provided key, so sort/order and lookups work.
        let schema = dyn_schema!(("id", UInt64, false), 0);
        let record = DynRecord::new(vec![Value::UInt64(42_u64)], 0);
        let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 2);
        let key = crate::version::timestamp::Ts {
            ts: 0.into(),
            value: record.key(),
        };

        // First a normal row, then a tombstone for the same key
        builder.push(key.clone(), Some(record.as_record_ref()));
        builder.push(key.clone(), None);

        let arrays = builder.finish(None);
        let rb = arrays.as_record_batch();
        // Column 2 is the primary key (after _null and ts)
        let pk_col = rb.column(2).as_primitive::<arrow::datatypes::UInt64Type>();
        // Row 1 is the tombstone
        assert_eq!(pk_col.value(1), 42_u64);
        // And the tombstone flag is set
        assert!(rb.column(0).as_boolean().value(1));
    }

    #[test]
    fn test_build_array() {
        let schema = dyn_schema!(
            ("id", UInt32, false),
            ("bool", Boolean, true),
            ("bytes", Binary, true),
            ("none", Int64, true),
            ("str", Utf8, false),
            ("float32", Float32, false),
            ("float64", Float64, true),
            0
        );

        let record = DynRecord::new(
            vec![
                Value::UInt32(1_u32),
                Value::Boolean(true),
                Value::Binary(vec![1_u8, 2, 3, 4]),
                Value::Null,
                Value::String("tonbo".to_string()),
                Value::Float32(1.09_f32),
                Value::Float64(3.09_f64),
            ],
            0,
        );

        let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
        let key = crate::version::timestamp::Ts {
            ts: 0.into(),
            value: record.key(),
        };
        builder.push(key.clone(), Some(record.as_record_ref()));
        builder.push(key.clone(), None);
        builder.push(key, Some(record.as_record_ref()));
        let arrays = builder.finish(None);

        {
            let res = arrays.get(0, &ProjectionMask::all());
            let cols = res.unwrap().unwrap().columns;
            for (actual, expected) in cols.iter().zip(record.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
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

    #[test]
    fn test_build_fixed_size_binary() {
        let schema = DynSchema::new(
            &vec![
                DynamicField::new("four".into(), DataType::FixedSizeBinary(4), false),
                DynamicField::new("five".into(), DataType::FixedSizeBinary(5), true),
                DynamicField::new("three".into(), DataType::FixedSizeBinary(3), false),
            ][..],
            0,
        );

        let record1 = DynRecord::new(
            vec![
                Value::FixedSizeBinary(vec![1, 2, 3, 4], 4),
                Value::FixedSizeBinary(vec![1, 2, 3, 4, 5], 5),
                Value::FixedSizeBinary(vec![1, 2, 3], 3),
            ],
            0,
        );
        let record2 = DynRecord::new(
            vec![
                Value::FixedSizeBinary(vec![2, 3, 4, 5], 4),
                Value::Null,
                Value::FixedSizeBinary(vec![2, 3, 7], 3),
            ],
            0,
        );

        let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
        let key = crate::version::timestamp::Ts {
            ts: 0.into(),
            value: record1.key(),
        };
        let key2 = crate::version::timestamp::Ts {
            ts: 0.into(),
            value: record2.key(),
        };
        builder.push(key.clone(), Some(record1.as_record_ref()));
        builder.push(key, None);
        builder.push(key2, Some(record2.as_record_ref()));
        let arrays = builder.finish(None);

        {
            let res1 = arrays.get(0, &ProjectionMask::all());
            let cols = res1.unwrap().unwrap().columns;
            for (actual, expected) in cols.iter().zip(record1.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
            }

            let res2 = arrays.get(1, &ProjectionMask::all());
            assert!(res2.unwrap().is_none());

            let res3 = arrays.get(2, &ProjectionMask::all());
            let cols = res3.unwrap().unwrap().columns;
            for (actual, expected) in cols.iter().zip(record2.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn test_build_list() {
        let ty1 = DataType::List(Arc::new(Field::new("code", DataType::UInt16, true)));
        let ty2 = DataType::List(Arc::new(Field::new("cofloatde", DataType::Float32, true)));
        let ty3 = DataType::List(Arc::new(Field::new(
            "time",
            DataType::Time32(ArrowTimeUnit::Second),
            true,
        )));
        let schema = DynSchema::new(
            &vec![
                DynamicField::new("id".into(), DataType::UInt64, false),
                DynamicField::new("codes".into(), ty1.clone(), true),
                DynamicField::new("vector".into(), ty2.clone(), true),
                DynamicField::new("schedules".into(), ty3.clone(), true),
            ],
            0,
        );

        let record1 = DynRecord::new(
            vec![
                Value::UInt64(1),
                Value::List(
                    DataType::UInt16,
                    vec![
                        Arc::new(Value::UInt16(1)),
                        Arc::new(Value::UInt16(2)),
                        Arc::new(Value::UInt16(3)),
                        Arc::new(Value::UInt16(4)),
                    ],
                ),
                Value::List(
                    DataType::Float32,
                    vec![
                        Arc::new(Value::Float32(1.0)),
                        Arc::new(Value::Float32(2.0)),
                        Arc::new(Value::Float32(3.0)),
                        Arc::new(Value::Float32(4.0)),
                    ],
                ),
                Value::List(
                    DataType::Time32(ArrowTimeUnit::Second),
                    vec![
                        Arc::new(Value::Time32(1, TimeUnit::Second)),
                        Arc::new(Value::Time32(2, TimeUnit::Second)),
                        Arc::new(Value::Time32(3, TimeUnit::Second)),
                        Arc::new(Value::Time32(4, TimeUnit::Second)),
                    ],
                ),
            ],
            0,
        );

        let record2 = DynRecord::new(
            vec![
                Value::UInt64(2),
                Value::List(
                    DataType::UInt16,
                    vec![
                        Arc::new(Value::UInt16(2)),
                        Arc::new(Value::UInt16(3)),
                        Arc::new(Value::UInt16(4)),
                        Arc::new(Value::UInt16(5)),
                    ],
                ),
                Value::List(
                    DataType::Float32,
                    vec![
                        Arc::new(Value::Float32(1.1)),
                        Arc::new(Value::Float32(2.1)),
                        Arc::new(Value::Null),
                        Arc::new(Value::Float32(4.1)),
                    ],
                ),
                Value::Null,
            ],
            0,
        );

        let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
        let key = crate::version::timestamp::Ts {
            ts: 0.into(),
            value: record1.key(),
        };
        let key2 = crate::version::timestamp::Ts {
            ts: 0.into(),
            value: record2.key(),
        };
        builder.push(key.clone(), Some(record1.as_record_ref()));
        builder.push(key, None);
        builder.push(key2, Some(record2.as_record_ref()));
        let arrays = builder.finish(None);

        {
            let res1 = arrays.get(0, &ProjectionMask::all());
            let cols = res1.unwrap().unwrap().columns;
            for (actual, expected) in cols.iter().zip(record1.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
            }

            let res2 = arrays.get(1, &ProjectionMask::all());
            assert!(res2.unwrap().is_none());

            let res3 = arrays.get(2, &ProjectionMask::all());
            let cols = res3.unwrap().unwrap().columns;
            for (actual, expected) in cols.iter().zip(record2.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn test_build_dict() {
        let ty1 = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let ty2 = DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
        let ty3 = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let ty4 = DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Binary));
        let schema = DynSchema::new(
            &vec![
                DynamicField::new("id".into(), DataType::UInt64, false),
                DynamicField::new("types".into(), ty1.clone(), true),
                DynamicField::new("countries".into(), ty2.clone(), true),
                DynamicField::new("cities".into(), ty3.clone(), true),
                DynamicField::new("bytes".into(), ty4.clone(), true),
            ],
            0,
        );

        let record1 = DynRecord::new(
            vec![
                Value::UInt64(1),
                Value::Dictionary(
                    DictionaryKeyType::Int8,
                    Box::new(Value::String("String".to_string())),
                ),
                Value::Dictionary(
                    DictionaryKeyType::Int16,
                    Box::new(Value::String("China".to_string())),
                ),
                Value::Dictionary(
                    DictionaryKeyType::Int32,
                    Box::new(Value::String("Shanghai".to_string())),
                ),
                Value::Dictionary(
                    DictionaryKeyType::Int64,
                    Box::new(Value::Binary(b"tonbo".to_vec())),
                ),
            ],
            0,
        );
        let record2 = DynRecord::new(
            vec![
                Value::UInt64(1),
                Value::Dictionary(DictionaryKeyType::Int8, Box::new(Value::Null)),
                Value::Null,
                Value::Dictionary(
                    DictionaryKeyType::Int32,
                    Box::new(Value::String("Tokyo".to_string())),
                ),
                Value::Null,
            ],
            0,
        );
        let mut builder = DynRecordImmutableArrays::builder(schema.arrow_schema().clone(), 5);
        let key = crate::version::timestamp::Ts {
            ts: 0.into(),
            value: record1.key(),
        };
        let key2 = crate::version::timestamp::Ts {
            ts: 0.into(),
            value: record2.key(),
        };
        builder.push(key.clone(), Some(record1.as_record_ref()));
        builder.push(key, None);
        builder.push(key2, Some(record2.as_record_ref()));
        let arrays = builder.finish(None);

        {
            let res1 = arrays.get(0, &ProjectionMask::all());
            let cols = res1.unwrap().unwrap().columns;
            for (actual, expected) in cols.iter().zip(record1.as_record_ref().columns.iter()) {
                assert_eq!(actual, expected);
            }

            let res2 = arrays.get(1, &ProjectionMask::all());
            assert!(res2.unwrap().is_none());

            let res3 = arrays.get(2, &ProjectionMask::all());
            let cols = res3.unwrap().unwrap().columns;
            for (actual, expected) in cols.iter().zip(record2.as_record_ref().columns.iter()) {
                match (actual, expected) {
                    (ValueRef::Null, ValueRef::Null) => continue,
                    (ValueRef::Null, ValueRef::Dictionary(_, value)) => {
                        assert!(value.is_null());
                    }
                    (ValueRef::Dictionary(_, value), ValueRef::Null) => {
                        assert!(value.is_null());
                    }
                    _ => assert_eq!(actual, expected),
                }
            }
        }
    }
}
