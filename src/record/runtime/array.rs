use std::{any::Any, mem, sync::Arc};

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, ArrowPrimitiveType, BooleanArray, BooleanBufferBuilder,
        BooleanBuilder, GenericBinaryArray, GenericBinaryBuilder, PrimitiveArray, PrimitiveBuilder,
        StringArray, StringBuilder, UInt32Builder,
    },
    datatypes::{
        Int16Type, Int32Type, Int64Type, Int8Type, Schema, UInt16Type, UInt32Type, UInt64Type,
        UInt8Type,
    },
};

use super::{column::Column, record::DynRecord, record_ref::DynRecordRef, Datatype};
use crate::{
    inmem::immutable::{ArrowArrays, Builder},
    record::{Key, Record},
    timestamp::Timestamped,
};

#[allow(unused)]
pub struct DynRecordImmutableArrays {
    _null: Arc<arrow::array::BooleanArray>,
    _ts: Arc<arrow::array::UInt32Array>,
    columns: Vec<Column>,
    record_batch: arrow::record_batch::RecordBatch,
}

impl ArrowArrays for DynRecordImmutableArrays {
    type Record = DynRecord;

    type Builder = DynRecordBuilder;

    fn builder(schema: &Arc<Schema>, capacity: usize) -> Self::Builder {
        let mut builders: Vec<Box<dyn ArrayBuilder>> = vec![];
        let mut datatypes = vec![];
        for field in schema.fields().iter().skip(2) {
            let datatype = Datatype::from(field.data_type());
            match datatype {
                Datatype::UInt8 => {
                    builders.push(Box::new(PrimitiveBuilder::<UInt8Type>::with_capacity(
                        capacity,
                    )));
                }
                Datatype::UInt16 => {
                    builders.push(Box::new(PrimitiveBuilder::<UInt16Type>::with_capacity(
                        capacity,
                    )));
                }
                Datatype::UInt32 => {
                    builders.push(Box::new(PrimitiveBuilder::<UInt32Type>::with_capacity(
                        capacity,
                    )));
                }
                Datatype::UInt64 => {
                    builders.push(Box::new(PrimitiveBuilder::<UInt64Type>::with_capacity(
                        capacity,
                    )));
                }
                Datatype::Int8 => {
                    builders.push(Box::new(PrimitiveBuilder::<Int8Type>::with_capacity(
                        capacity,
                    )));
                }
                Datatype::Int16 => {
                    builders.push(Box::new(PrimitiveBuilder::<Int16Type>::with_capacity(
                        capacity,
                    )));
                }
                Datatype::Int32 => {
                    builders.push(Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(
                        capacity,
                    )));
                }
                Datatype::Int64 => {
                    builders.push(Box::new(PrimitiveBuilder::<Int64Type>::with_capacity(
                        capacity,
                    )));
                }
                Datatype::String => {
                    builders.push(Box::new(StringBuilder::with_capacity(capacity, 0)));
                }
                Datatype::Boolean => {
                    builders.push(Box::new(BooleanBuilder::with_capacity(capacity)));
                }
                Datatype::Bytes => {
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

        let mut columns = vec![];
        for (idx, col) in self.columns.iter().enumerate() {
            if projection_mask.leaf_included(idx + 2) && !col.is_nullable {
                let datatype = col.datatype;
                let name = col.name.to_string();
                let value: Arc<dyn Any> = match datatype {
                    Datatype::UInt8 => Arc::new(Self::primitive_value::<UInt8Type>(col, offset)),
                    Datatype::UInt16 => Arc::new(Self::primitive_value::<UInt16Type>(col, offset)),
                    Datatype::UInt32 => Arc::new(Self::primitive_value::<UInt32Type>(col, offset)),
                    Datatype::UInt64 => Arc::new(Self::primitive_value::<UInt64Type>(col, offset)),
                    Datatype::Int8 => Arc::new(Self::primitive_value::<Int8Type>(col, offset)),
                    Datatype::Int16 => Arc::new(Self::primitive_value::<Int16Type>(col, offset)),
                    Datatype::Int32 => Arc::new(Self::primitive_value::<Int32Type>(col, offset)),
                    Datatype::Int64 => Arc::new(Self::primitive_value::<Int64Type>(col, offset)),
                    Datatype::String => Arc::new(
                        col.value
                            .as_ref()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(offset)
                            .to_owned(),
                    ),
                    Datatype::Boolean => Arc::new(
                        col.value
                            .as_ref()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(offset),
                    ),
                    Datatype::Bytes => Arc::new(
                        col.value
                            .as_ref()
                            .downcast_ref::<GenericBinaryArray<i32>>()
                            .unwrap()
                            .value(offset)
                            .to_owned(),
                    ),
                };
                columns.push(Column {
                    datatype,
                    name,
                    value,
                    is_nullable: true,
                });
            }

            columns.push(col.clone());
        }
        Some(Some(DynRecordRef::new(columns, 2)))
    }

    fn as_record_batch(&self) -> &arrow::array::RecordBatch {
        &self.record_batch
    }
}
impl DynRecordImmutableArrays {
    fn primitive_value<T>(col: &Column, offset: usize) -> T::Native
    where
        T: ArrowPrimitiveType,
    {
        col.value
            .as_ref()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap()
            .value(offset)
    }
}

pub struct DynRecordBuilder {
    builders: Vec<Box<dyn ArrayBuilder>>,
    datatypes: Vec<Datatype>,
    _null: BooleanBufferBuilder,
    _ts: UInt32Builder,
    schema: Arc<Schema>,
}

impl Builder<DynRecordImmutableArrays> for DynRecordBuilder {
    fn push(
        &mut self,
        key: Timestamped<<<DynRecord as Record>::Key as Key>::Ref<'_>>,
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
                    let datatype = col.datatype;
                    match datatype {
                        Datatype::UInt8 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<UInt8Type>>(builder);
                            let value = col.value.as_ref().downcast_ref::<Option<u8>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::UInt16 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<UInt16Type>>(builder);
                            let value = col.value.as_ref().downcast_ref::<Option<u16>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::UInt32 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<UInt32Type>>(builder);
                            let value = col.value.as_ref().downcast_ref::<Option<u32>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::UInt64 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<UInt64Type>>(builder);
                            let value = col.value.as_ref().downcast_ref::<Option<u64>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::Int8 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<Int8Type>>(builder);
                            let value = col.value.as_ref().downcast_ref::<Option<i8>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::Int16 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<Int16Type>>(builder);
                            let value = col.value.as_ref().downcast_ref::<Option<i16>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::Int32 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<Int32Type>>(builder);
                            let value = col.value.as_ref().downcast_ref::<Option<i32>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::Int64 => {
                            let bd = Self::as_builder_mut::<PrimitiveBuilder<Int64Type>>(builder);
                            let value = col.value.as_ref().downcast_ref::<Option<i64>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::String => {
                            let bd = Self::as_builder_mut::<StringBuilder>(builder);
                            let value =
                                col.value.as_ref().downcast_ref::<Option<String>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::Boolean => {
                            let bd = Self::as_builder_mut::<BooleanBuilder>(builder);
                            let value = col.value.as_ref().downcast_ref::<Option<bool>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::Bytes => {
                            let bd = Self::as_builder_mut::<GenericBinaryBuilder<i32>>(builder);
                            let value = col
                                .value
                                .as_ref()
                                .downcast_ref::<Option<Vec<u8>>>()
                                .unwrap();
                            match value {
                                Some(value) => bd.append_value(value),
                                None => bd.append_null(),
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
                        Datatype::UInt8 => {
                            Self::as_builder_mut::<PrimitiveBuilder<UInt8Type>>(builder)
                                .append_value(u8::default());
                        }
                        Datatype::UInt16 => {
                            Self::as_builder_mut::<PrimitiveBuilder<UInt16Type>>(builder)
                                .append_value(u16::default());
                        }
                        Datatype::UInt32 => {
                            Self::as_builder_mut::<PrimitiveBuilder<UInt32Type>>(builder)
                                .append_value(u32::default());
                        }
                        Datatype::UInt64 => {
                            Self::as_builder_mut::<PrimitiveBuilder<UInt64Type>>(builder)
                                .append_value(u64::default());
                        }
                        Datatype::Int8 => {
                            Self::as_builder_mut::<PrimitiveBuilder<Int8Type>>(builder)
                                .append_value(i8::default());
                        }
                        Datatype::Int16 => {
                            Self::as_builder_mut::<PrimitiveBuilder<Int16Type>>(builder)
                                .append_value(i16::default());
                        }
                        Datatype::Int32 => {
                            Self::as_builder_mut::<PrimitiveBuilder<Int32Type>>(builder)
                                .append_value(i32::default());
                        }
                        Datatype::Int64 => {
                            Self::as_builder_mut::<PrimitiveBuilder<Int64Type>>(builder)
                                .append_value(i64::default());
                        }
                        Datatype::String => {
                            Self::as_builder_mut::<StringBuilder>(builder)
                                .append_value(String::default());
                        }
                        Datatype::Boolean => {
                            Self::as_builder_mut::<BooleanBuilder>(builder)
                                .append_value(bool::default());
                        }
                        Datatype::Bytes => {
                            Self::as_builder_mut::<GenericBinaryBuilder<i32>>(builder)
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
                    Datatype::UInt8 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<UInt8Type>>(builder).values_slice(),
                    ),
                    Datatype::UInt16 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<UInt16Type>>(builder).values_slice(),
                    ),
                    Datatype::UInt32 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<UInt32Type>>(builder).values_slice(),
                    ),
                    Datatype::UInt64 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<UInt64Type>>(builder).values_slice(),
                    ),
                    Datatype::Int8 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<Int8Type>>(builder).values_slice(),
                    ),
                    Datatype::Int16 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<Int16Type>>(builder).values_slice(),
                    ),
                    Datatype::Int32 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<Int32Type>>(builder).values_slice(),
                    ),
                    Datatype::Int64 => mem::size_of_val(
                        Self::as_builder::<PrimitiveBuilder<Int64Type>>(builder).values_slice(),
                    ),
                    Datatype::String => {
                        mem::size_of_val(Self::as_builder::<StringBuilder>(builder).values_slice())
                    }
                    Datatype::Boolean => {
                        mem::size_of_val(Self::as_builder::<BooleanBuilder>(builder).values_slice())
                    }
                    Datatype::Bytes => mem::size_of_val(
                        Self::as_builder::<GenericBinaryBuilder<i32>>(builder).values_slice(),
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
            let field = self.schema.field(idx + 2);
            let is_nullable = field.is_nullable();
            match datatype {
                Datatype::UInt8 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<UInt8Type>>(builder).finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::UInt8,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::UInt16 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<UInt16Type>>(builder).finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::UInt16,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::UInt32 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<UInt32Type>>(builder).finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::UInt32,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::UInt64 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<UInt64Type>>(builder).finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::UInt64,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::Int8 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<Int8Type>>(builder).finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::Int8,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::Int16 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<Int16Type>>(builder).finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::Int16,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::Int32 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<Int32Type>>(builder).finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::Int32,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::Int64 => {
                    let value = Arc::new(
                        Self::as_builder_mut::<PrimitiveBuilder<Int64Type>>(builder).finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::Int64,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::String => {
                    let value = Arc::new(Self::as_builder_mut::<StringBuilder>(builder).finish());
                    columns.push(Column {
                        datatype: Datatype::String,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::Boolean => {
                    let value = Arc::new(Self::as_builder_mut::<BooleanBuilder>(builder).finish());
                    columns.push(Column {
                        datatype: Datatype::Boolean,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::Bytes => {
                    let value = Arc::new(
                        Self::as_builder_mut::<GenericBinaryBuilder<i32>>(builder).finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::Bytes,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
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
        key: Timestamped<<<DynRecord as Record>::Key as Key>::Ref<'_>>,
        primary_key_index: usize,
    ) {
        let builder = self.builders.get_mut(primary_key_index).unwrap();
        let datatype = self.datatypes.get_mut(primary_key_index).unwrap();
        let col = key.value;
        match datatype {
            Datatype::UInt8 => Self::as_builder_mut::<PrimitiveBuilder<UInt8Type>>(builder)
                .append_value(*col.value.as_ref().downcast_ref::<u8>().unwrap()),
            Datatype::UInt16 => Self::as_builder_mut::<PrimitiveBuilder<UInt16Type>>(builder)
                .append_value(*col.value.as_ref().downcast_ref::<u16>().unwrap()),
            Datatype::UInt32 => Self::as_builder_mut::<PrimitiveBuilder<UInt32Type>>(builder)
                .append_value(*col.value.as_ref().downcast_ref::<u32>().unwrap()),
            Datatype::UInt64 => Self::as_builder_mut::<PrimitiveBuilder<UInt64Type>>(builder)
                .append_value(*col.value.as_ref().downcast_ref::<u64>().unwrap()),
            Datatype::Int8 => Self::as_builder_mut::<PrimitiveBuilder<Int8Type>>(builder)
                .append_value(*col.value.as_ref().downcast_ref::<i8>().unwrap()),
            Datatype::Int16 => Self::as_builder_mut::<PrimitiveBuilder<Int16Type>>(builder)
                .append_value(*col.value.as_ref().downcast_ref::<i16>().unwrap()),
            Datatype::Int32 => Self::as_builder_mut::<PrimitiveBuilder<Int32Type>>(builder)
                .append_value(*col.value.as_ref().downcast_ref::<i32>().unwrap()),
            Datatype::Int64 => Self::as_builder_mut::<PrimitiveBuilder<Int64Type>>(builder)
                .append_value(*col.value.as_ref().downcast_ref::<i64>().unwrap()),
            Datatype::String => Self::as_builder_mut::<StringBuilder>(builder)
                .append_value(col.value.as_ref().downcast_ref::<String>().unwrap()),
            Datatype::Boolean => Self::as_builder_mut::<BooleanBuilder>(builder)
                .append_value(*col.value.as_ref().downcast_ref::<bool>().unwrap()),
            Datatype::Bytes => Self::as_builder_mut::<GenericBinaryBuilder<i32>>(builder)
                .append_value(col.value.as_ref().downcast_ref::<Vec<u8>>().unwrap()),
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

unsafe impl Send for DynRecordBuilder {}
unsafe impl Sync for DynRecordBuilder {}

unsafe impl Send for DynRecordImmutableArrays {}
unsafe impl Sync for DynRecordImmutableArrays {}
