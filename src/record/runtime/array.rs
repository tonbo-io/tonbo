use std::{any::Any, mem, sync::Arc};

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, ArrowPrimitiveType, BooleanArray, BooleanBufferBuilder,
        BooleanBuilder, GenericBinaryArray, GenericBinaryBuilder, PrimitiveArray, PrimitiveBuilder,
        StringArray, StringBuilder, UInt32Builder,
    },
    datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema as ArrowSchema,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};

use super::{record::DynRecord, record_ref::DynRecordRef, value::Value, DataType};
use crate::{
    cast_arc_value,
    inmem::immutable::{ArrowArrays, Builder},
    magic::USER_COLUMN_OFFSET,
    record::{Key, Record, Schema, F32, F64},
    timestamp::Timestamped,
};

#[allow(unused)]
pub struct DynRecordImmutableArrays {
    _null: Arc<arrow::array::BooleanArray>,
    _ts: Arc<arrow::array::UInt32Array>,
    columns: Vec<Value>,
    record_batch: arrow::record_batch::RecordBatch,
}

impl ArrowArrays for DynRecordImmutableArrays {
    type Record = DynRecord;

    type Builder = DynRecordBuilder;

    fn builder(schema: Arc<ArrowSchema>, capacity: usize) -> Self::Builder {
        let mut builders: Vec<Box<dyn ArrayBuilder + Send + Sync>> = vec![];
        let mut datatypes = vec![];
        for field in schema.fields().iter().skip(2) {
            let datatype = DataType::from(field.data_type());
            match &datatype {
                DataType::UInt8 => {
                    builders.push(Box::new(PrimitiveBuilder::<UInt8Type>::with_capacity(
                        capacity,
                    )));
                }
                DataType::UInt16 => {
                    builders.push(Box::new(PrimitiveBuilder::<UInt16Type>::with_capacity(
                        capacity,
                    )));
                }
                DataType::UInt32 => {
                    builders.push(Box::new(PrimitiveBuilder::<UInt32Type>::with_capacity(
                        capacity,
                    )));
                }
                DataType::UInt64 => {
                    builders.push(Box::new(PrimitiveBuilder::<UInt64Type>::with_capacity(
                        capacity,
                    )));
                }
                DataType::Int8 => {
                    builders.push(Box::new(PrimitiveBuilder::<Int8Type>::with_capacity(
                        capacity,
                    )));
                }
                DataType::Int16 => {
                    builders.push(Box::new(PrimitiveBuilder::<Int16Type>::with_capacity(
                        capacity,
                    )));
                }
                DataType::Int32 => {
                    builders.push(Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(
                        capacity,
                    )));
                }
                DataType::Int64 => {
                    builders.push(Box::new(PrimitiveBuilder::<Int64Type>::with_capacity(
                        capacity,
                    )));
                }
                DataType::Float32 => {
                    builders.push(Box::new(PrimitiveBuilder::<Float32Type>::with_capacity(
                        capacity,
                    )));
                }
                DataType::Float64 => {
                    builders.push(Box::new(PrimitiveBuilder::<Float64Type>::with_capacity(
                        capacity,
                    )));
                }
                DataType::String => {
                    builders.push(Box::new(StringBuilder::with_capacity(capacity, 0)));
                }
                DataType::Boolean => {
                    builders.push(Box::new(BooleanBuilder::with_capacity(capacity)));
                }
                DataType::Bytes => {
                    builders.push(Box::new(GenericBinaryBuilder::<i32>::with_capacity(
                        capacity, 0,
                    )));
                }
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
                    DataType::UInt8 => {
                        let v = Self::primitive_value::<UInt8Type>(col, offset);
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
                    DataType::UInt16 => {
                        let v = Self::primitive_value::<UInt16Type>(col, offset);
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
                    DataType::UInt32 => {
                        let v = Self::primitive_value::<UInt32Type>(col, offset);
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
                    DataType::UInt64 => {
                        let v = Self::primitive_value::<UInt64Type>(col, offset);
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
                    DataType::Int8 => {
                        let v = Self::primitive_value::<Int8Type>(col, offset);
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
                    DataType::Int16 => {
                        let v = Self::primitive_value::<Int16Type>(col, offset);
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
                    DataType::Int32 => {
                        let v = Self::primitive_value::<Int32Type>(col, offset);
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
                    DataType::Int64 => {
                        let v = Self::primitive_value::<Int64Type>(col, offset);
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
                    DataType::Float32 => {
                        let v = Self::primitive_value::<Float32Type>(col, offset);
                        if primary_key_index == idx {
                            Arc::new(F32::from(v))
                        } else {
                            Arc::new(Some(F32::from(v)))
                        }
                    }
                    DataType::Float64 => {
                        let v = Self::primitive_value::<Float64Type>(col, offset);
                        if primary_key_index == idx {
                            Arc::new(F64::from(v))
                        } else {
                            Arc::new(Some(F64::from(v)))
                        }
                    }
                    DataType::String => {
                        let v = cast_arc_value!(col.value, StringArray)
                            .value(offset)
                            .to_owned();
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
                    DataType::Boolean => {
                        let v = cast_arc_value!(col.value, BooleanArray).value(offset);
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
                    DataType::Bytes => {
                        let v = cast_arc_value!(col.value, GenericBinaryArray<i32>)
                            .value(offset)
                            .to_owned();
                        if primary_key_index == idx {
                            Arc::new(v)
                        } else {
                            Arc::new(Some(v))
                        }
                    }
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
impl DynRecordImmutableArrays {
    fn primitive_value<T>(col: &Value, offset: usize) -> T::Native
    where
        T: ArrowPrimitiveType,
    {
        cast_arc_value!(col.value, PrimitiveArray<T>).value(offset)
    }
}

pub struct DynRecordBuilder {
    builders: Vec<Box<dyn ArrayBuilder + Send + Sync>>,
    datatypes: Vec<DataType>,
    _null: BooleanBufferBuilder,
    _ts: UInt32Builder,
    schema: Arc<ArrowSchema>,
}

impl Builder<DynRecordImmutableArrays> for DynRecordBuilder {
    fn push(
        &mut self,
        key: Timestamped<<<<DynRecord as Record>::Schema as Schema>::Key as Key>::Ref<'_>>,
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
                        DataType::UInt8 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<UInt8Type>>(
                                builder.as_mut(),
                            );
                            match cast_arc_value!(col.value, Option<u8>) {
                                Some(value) => bd.append_value(*value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::UInt16 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<UInt16Type>>(
                                builder.as_mut(),
                            );
                            match cast_arc_value!(col.value, Option<u16>) {
                                Some(value) => bd.append_value(*value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::UInt32 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<UInt32Type>>(
                                builder.as_mut(),
                            );
                            match cast_arc_value!(col.value, Option<u32>) {
                                Some(value) => bd.append_value(*value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::UInt64 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<UInt64Type>>(
                                builder.as_mut(),
                            );
                            match cast_arc_value!(col.value, Option<u64>) {
                                Some(value) => bd.append_value(*value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::Int8 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<Int8Type>>(
                                builder.as_mut(),
                            );
                            match cast_arc_value!(col.value, Option<i8>) {
                                Some(value) => bd.append_value(*value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::Int16 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<Int16Type>>(
                                builder.as_mut(),
                            );
                            match cast_arc_value!(col.value, Option<i16>) {
                                Some(value) => bd.append_value(*value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::Int32 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<Int32Type>>(
                                builder.as_mut(),
                            );
                            match cast_arc_value!(col.value, Option<i32>) {
                                Some(value) => bd.append_value(*value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::Int64 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<Int64Type>>(
                                builder.as_mut(),
                            );
                            match cast_arc_value!(col.value, Option<i64>) {
                                Some(value) => bd.append_value(*value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::Float32 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<Float32Type>>(
                                builder.as_mut(),
                            );
                            match cast_arc_value!(col.value, Option<F32>) {
                                Some(value) => bd.append_value(value.into()),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::Float64 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<Float64Type>>(
                                builder.as_mut(),
                            );
                            match cast_arc_value!(col.value, Option<F64>) {
                                Some(value) => bd.append_value(value.into()),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::String => {
                            let bd = Self::as_builder_mut::<StringBuilder>(builder.as_mut());
                            match cast_arc_value!(col.value, Option<String>) {
                                Some(value) => bd.append_value(value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(""),
                            }
                        }
                        DataType::Boolean => {
                            let bd = Self::as_builder_mut::<BooleanBuilder>(builder.as_mut());
                            match cast_arc_value!(col.value, Option<bool>) {
                                Some(value) => bd.append_value(*value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(Default::default()),
                            }
                        }
                        DataType::Bytes => {
                            let bd =
                                Self::as_builder_mut::<GenericBinaryBuilder<i32>>(builder.as_mut());
                            match cast_arc_value!(col.value, Option<Vec<u8>>) {
                                Some(value) => bd.append_value(value),
                                None if col.is_nullable() => bd.append_null(),
                                None => bd.append_value(vec![]),
                            }
                        }
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
                        DataType::UInt8 => {
                            Self::as_builder_mut::<PrimitiveBuilder<UInt8Type>>(builder.as_mut())
                                .append_value(u8::default());
                        }
                        DataType::UInt16 => {
                            Self::as_builder_mut::<PrimitiveBuilder<UInt16Type>>(builder.as_mut())
                                .append_value(u16::default());
                        }
                        DataType::UInt32 => {
                            Self::as_builder_mut::<PrimitiveBuilder<UInt32Type>>(builder.as_mut())
                                .append_value(u32::default());
                        }
                        DataType::UInt64 => {
                            Self::as_builder_mut::<PrimitiveBuilder<UInt64Type>>(builder.as_mut())
                                .append_value(u64::default());
                        }
                        DataType::Int8 => {
                            Self::as_builder_mut::<PrimitiveBuilder<Int8Type>>(builder.as_mut())
                                .append_value(i8::default());
                        }
                        DataType::Int16 => {
                            Self::as_builder_mut::<PrimitiveBuilder<Int16Type>>(builder.as_mut())
                                .append_value(i16::default());
                        }
                        DataType::Int32 => {
                            Self::as_builder_mut::<PrimitiveBuilder<Int32Type>>(builder.as_mut())
                                .append_value(i32::default());
                        }
                        DataType::Int64 => {
                            Self::as_builder_mut::<PrimitiveBuilder<Int64Type>>(builder.as_mut())
                                .append_value(i64::default());
                        }
                        DataType::Float32 => {
                            Self::as_builder_mut::<PrimitiveBuilder<Float32Type>>(builder.as_mut())
                                .append_value(f32::default());
                        }
                        DataType::Float64 => {
                            Self::as_builder_mut::<PrimitiveBuilder<Float64Type>>(builder.as_mut())
                                .append_value(f64::default());
                        }
                        DataType::String => {
                            Self::as_builder_mut::<StringBuilder>(builder.as_mut())
                                .append_value(String::default());
                        }
                        DataType::Boolean => {
                            Self::as_builder_mut::<BooleanBuilder>(builder.as_mut())
                                .append_value(bool::default());
                        }
                        DataType::Bytes => {
                            Self::as_builder_mut::<GenericBinaryBuilder<i32>>(builder.as_mut())
                                .append_value(Vec::<u8>::default());
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
                    DataType::UInt8 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<UInt8Type>>(builder.as_ref())
                            .values_slice(),
                    ),
                    DataType::UInt16 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<UInt16Type>>(builder.as_ref())
                            .values_slice(),
                    ),
                    DataType::UInt32 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<UInt32Type>>(builder.as_ref())
                            .values_slice(),
                    ),
                    DataType::UInt64 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<UInt64Type>>(builder.as_ref())
                            .values_slice(),
                    ),
                    DataType::Int8 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<Int8Type>>(builder.as_ref())
                            .values_slice(),
                    ),
                    DataType::Int16 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<Int16Type>>(builder.as_ref())
                            .values_slice(),
                    ),
                    DataType::Int32 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<Int32Type>>(builder.as_ref())
                            .values_slice(),
                    ),
                    DataType::Int64 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<Int64Type>>(builder.as_ref())
                            .values_slice(),
                    ),
                    DataType::Float32 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<Float32Type>>(builder.as_ref())
                            .values_slice(),
                    ),
                    DataType::Float64 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<Float64Type>>(builder.as_ref())
                            .values_slice(),
                    ),
                    DataType::String => mem::size_of_val(
                        Self::as_builder::<StringBuilder>(builder.as_ref()).values_slice(),
                    ),
                    DataType::Boolean => mem::size_of_val(
                        Self::as_builder::<BooleanBuilder>(builder.as_ref()).values_slice(),
                    ),
                    DataType::Bytes => mem::size_of_val(
                        Self::as_builder::<GenericBinaryBuilder<i32>>(builder.as_ref())
                            .values_slice(),
                    ),
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
                DataType::UInt8 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<UInt8Type>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::UInt8,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
                DataType::UInt16 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<UInt16Type>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::UInt16,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
                DataType::UInt32 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<UInt32Type>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::UInt32,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
                DataType::UInt64 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<UInt64Type>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::UInt64,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
                DataType::Int8 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<Int8Type>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::Int8,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
                DataType::Int16 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<Int16Type>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::Int16,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
                DataType::Int32 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<Int32Type>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::Int32,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
                DataType::Int64 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<Int64Type>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::Int64,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
                DataType::Float32 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<Float32Type>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::Float32,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
                DataType::Float64 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<Float64Type>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::Float64,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
                DataType::String => {
                    let value =
                        Arc::new(Self::as_builder_mut::<StringBuilder>(builder.as_mut()).finish());
                    columns.push(Value::new(
                        DataType::String,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
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
                DataType::Bytes => {
                    let value = Arc::new(
                        Self::as_builder_mut::<GenericBinaryBuilder<i32>>(builder.as_mut())
                            .finish(),
                    );
                    columns.push(Value::new(
                        DataType::Bytes,
                        field.name().to_owned(),
                        value.clone(),
                        is_nullable,
                    ));
                    array_refs.push(value);
                }
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
        key: Timestamped<<<<DynRecord as Record>::Schema as Schema>::Key as Key>::Ref<'_>>,
        primary_key_index: usize,
    ) {
        let builder = self.builders.get_mut(primary_key_index).unwrap();
        let datatype = self.datatypes.get_mut(primary_key_index).unwrap();
        let col = key.value;
        match datatype {
            DataType::UInt8 => {
                Self::as_builder_mut::<PrimitiveBuilder<UInt8Type>>(builder.as_mut())
                    .append_value(*cast_arc_value!(col.value, u8))
            }
            DataType::UInt16 => {
                Self::as_builder_mut::<PrimitiveBuilder<UInt16Type>>(builder.as_mut())
                    .append_value(*cast_arc_value!(col.value, u16))
            }
            DataType::UInt32 => {
                Self::as_builder_mut::<PrimitiveBuilder<UInt32Type>>(builder.as_mut())
                    .append_value(*cast_arc_value!(col.value, u32))
            }
            DataType::UInt64 => {
                Self::as_builder_mut::<PrimitiveBuilder<UInt64Type>>(builder.as_mut())
                    .append_value(*cast_arc_value!(col.value, u64))
            }
            DataType::Int8 => Self::as_builder_mut::<PrimitiveBuilder<Int8Type>>(builder.as_mut())
                .append_value(*cast_arc_value!(col.value, i8)),
            DataType::Int16 => {
                Self::as_builder_mut::<PrimitiveBuilder<Int16Type>>(builder.as_mut())
                    .append_value(*cast_arc_value!(col.value, i16))
            }
            DataType::Int32 => {
                Self::as_builder_mut::<PrimitiveBuilder<Int32Type>>(builder.as_mut())
                    .append_value(*cast_arc_value!(col.value, i32))
            }
            DataType::Int64 => {
                Self::as_builder_mut::<PrimitiveBuilder<Int64Type>>(builder.as_mut())
                    .append_value(*cast_arc_value!(col.value, i64))
            }
            DataType::Float32 => {
                Self::as_builder_mut::<PrimitiveBuilder<Float32Type>>(builder.as_mut())
                    .append_value(*cast_arc_value!(col.value, f32))
            }
            DataType::Float64 => {
                Self::as_builder_mut::<PrimitiveBuilder<Float64Type>>(builder.as_mut())
                    .append_value(*cast_arc_value!(col.value, f64))
            }
            DataType::String => Self::as_builder_mut::<StringBuilder>(builder.as_mut())
                .append_value(cast_arc_value!(col.value, String)),
            DataType::Boolean => Self::as_builder_mut::<BooleanBuilder>(builder.as_mut())
                .append_value(*cast_arc_value!(col.value, bool)),
            DataType::Bytes => Self::as_builder_mut::<GenericBinaryBuilder<i32>>(builder.as_mut())
                .append_value(cast_arc_value!(col.value, Vec<u8>)),
        };
    }

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

#[cfg(test)]
mod tests {

    use parquet::arrow::ProjectionMask;

    use crate::{
        dyn_record, dyn_schema,
        inmem::immutable::{ArrowArrays, Builder},
        record::{DynRecordImmutableArrays, DynRecordRef, Record, RecordRef, Schema, F32, F64},
    };

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
        let key = crate::timestamp::Timestamped {
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
            dbg!(&cols);
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
