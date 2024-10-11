use std::{any::Any, mem, sync::Arc};

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBufferBuilder, BooleanBuilder,
        Int16Array, Int32Array, Int64Array, Int8Array, PrimitiveBuilder, StringArray,
        StringBuilder, UInt32Builder,
    },
    datatypes::{Int16Type, Int32Type, Int64Type, Int8Type, Schema},
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
                    Datatype::Int8 => Arc::new(
                        col.value
                            .as_ref()
                            .downcast_ref::<Int8Array>()
                            .unwrap()
                            .value(offset),
                    ),
                    Datatype::Int16 => Arc::new(
                        col.value
                            .as_ref()
                            .downcast_ref::<Int16Array>()
                            .unwrap()
                            .value(offset),
                    ),
                    Datatype::Int32 => Arc::new(
                        col.value
                            .as_ref()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(offset),
                    ),
                    Datatype::Int64 => Arc::new(
                        col.value
                            .as_ref()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(offset),
                    ),
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
                        Datatype::Int8 => {
                            let bd = builder
                                .as_any_mut()
                                .downcast_mut::<PrimitiveBuilder<Int8Type>>()
                                .unwrap();

                            let value = col.value.as_ref().downcast_ref::<Option<i8>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::Int16 => {
                            let bd = builder
                                .as_any_mut()
                                .downcast_mut::<PrimitiveBuilder<Int16Type>>()
                                .unwrap();
                            let value = col.value.as_ref().downcast_ref::<Option<i16>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::Int32 => {
                            let bd = builder
                                .as_any_mut()
                                .downcast_mut::<PrimitiveBuilder<Int32Type>>()
                                .unwrap();
                            let value = col.value.as_ref().downcast_ref::<Option<i32>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::Int64 => {
                            let bd = builder
                                .as_any_mut()
                                .downcast_mut::<PrimitiveBuilder<Int64Type>>()
                                .unwrap();
                            let value = col.value.as_ref().downcast_ref::<Option<i64>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::String => {
                            let bd = builder
                                .as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .unwrap();
                            let value =
                                col.value.as_ref().downcast_ref::<Option<String>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(value),
                                None => bd.append_null(),
                            }
                        }
                        Datatype::Boolean => {
                            let bd = builder
                                .as_any_mut()
                                .downcast_mut::<BooleanBuilder>()
                                .unwrap();
                            let value = col.value.as_ref().downcast_ref::<Option<bool>>().unwrap();
                            match value {
                                Some(value) => bd.append_value(*value),
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
                        Datatype::Int8 => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<PrimitiveBuilder<Int8Type>>()
                                .unwrap()
                                .append_value(i8::default());
                        }
                        Datatype::Int16 => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<PrimitiveBuilder<Int16Type>>()
                                .unwrap()
                                .append_value(i16::default());
                        }
                        Datatype::Int32 => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<PrimitiveBuilder<Int32Type>>()
                                .unwrap()
                                .append_value(i32::default());
                        }
                        Datatype::Int64 => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<PrimitiveBuilder<Int64Type>>()
                                .unwrap()
                                .append_value(i64::default());
                        }
                        Datatype::String => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .unwrap()
                                .append_value(String::default());
                        }
                        Datatype::Boolean => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<BooleanBuilder>()
                                .unwrap()
                                .append_value(bool::default());
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
                    Datatype::Int8 => mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<PrimitiveBuilder<Int8Type>>()
                            .unwrap()
                            .values_slice(),
                    ),
                    Datatype::Int16 => mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<PrimitiveBuilder<Int16Type>>()
                            .unwrap()
                            .values_slice(),
                    ),
                    Datatype::Int32 => mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<PrimitiveBuilder<Int32Type>>()
                            .unwrap()
                            .values_slice(),
                    ),
                    Datatype::Int64 => mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<PrimitiveBuilder<Int64Type>>()
                            .unwrap()
                            .values_slice(),
                    ),
                    Datatype::String => mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<StringBuilder>()
                            .unwrap()
                            .values_slice(),
                    ),
                    Datatype::Boolean => mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<BooleanBuilder>()
                            .unwrap()
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
            let field = self.schema.field(idx + 2);
            let is_nullable = field.is_nullable();
            match datatype {
                Datatype::Int8 => {
                    let value = Arc::new(
                        builder
                            .as_any_mut()
                            .downcast_mut::<PrimitiveBuilder<Int8Type>>()
                            .unwrap()
                            .finish(),
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
                        builder
                            .as_any_mut()
                            .downcast_mut::<PrimitiveBuilder<Int16Type>>()
                            .unwrap()
                            .finish(),
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
                        builder
                            .as_any_mut()
                            .downcast_mut::<PrimitiveBuilder<Int32Type>>()
                            .unwrap()
                            .finish(),
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
                        builder
                            .as_any_mut()
                            .downcast_mut::<PrimitiveBuilder<Int64Type>>()
                            .unwrap()
                            .finish(),
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
                    let value = Arc::new(
                        builder
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap()
                            .finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::String,
                        name: field.name().to_owned(),
                        value: value.clone(),
                        is_nullable,
                    });
                    array_refs.push(value);
                }
                Datatype::Boolean => {
                    let value = Arc::new(
                        builder
                            .as_any_mut()
                            .downcast_mut::<BooleanBuilder>()
                            .unwrap()
                            .finish(),
                    );
                    columns.push(Column {
                        datatype: Datatype::Boolean,
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
        // *col.value.as_ref().downcast_ref::<i32>().unwrap()
        match datatype {
            Datatype::Int8 => builder
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<Int8Type>>()
                .unwrap()
                .append_value(*col.value.as_ref().downcast_ref::<i8>().unwrap()),
            Datatype::Int16 => builder
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<Int16Type>>()
                .unwrap()
                .append_value(*col.value.as_ref().downcast_ref::<i16>().unwrap()),
            Datatype::Int32 => builder
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<Int32Type>>()
                .unwrap()
                .append_value(*col.value.as_ref().downcast_ref::<i32>().unwrap()),
            Datatype::Int64 => builder
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<Int64Type>>()
                .unwrap()
                .append_value(*col.value.as_ref().downcast_ref::<i64>().unwrap()),
            Datatype::String => builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(col.value.as_ref().downcast_ref::<String>().unwrap()),
            Datatype::Boolean => builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap()
                .append_value(*col.value.as_ref().downcast_ref::<bool>().unwrap()),
        };
    }
}

unsafe impl Send for DynRecordBuilder {}
unsafe impl Sync for DynRecordBuilder {}

unsafe impl Send for DynRecordImmutableArrays {}
unsafe impl Sync for DynRecordImmutableArrays {}
