use std::{any::Any, sync::Arc};

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBufferBuilder, PrimitiveBuilder,
        UInt32Builder,
    },
    datatypes::{Int16Type, Int32Type, Int8Type, Schema},
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
                let value = match datatype {
                    Datatype::Int8 => {
                        Arc::new(col.value.as_ref().downcast_ref::<i8>().copied()) as Arc<dyn Any>
                    }
                    Datatype::Int16 => {
                        Arc::new(col.value.as_ref().downcast_ref::<i16>().copied()) as Arc<dyn Any>
                    }
                    Datatype::Int32 => {
                        Arc::new(col.value.as_ref().downcast_ref::<i32>().copied()) as Arc<dyn Any>
                    }
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
                    }
                }
            }
        }
    }

    fn written_size(&self) -> usize {
        let size = self._null.as_slice().len() + std::mem::size_of_val(self._ts.values_slice());
        self.builders
            .iter()
            .zip(self.datatypes.iter())
            .fold(size, |acc, (builder, datatype)| {
                acc + match datatype {
                    Datatype::Int8 => std::mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<PrimitiveBuilder<Int8Type>>()
                            .unwrap()
                            .values_slice(),
                    ),
                    Datatype::Int16 => std::mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<PrimitiveBuilder<Int16Type>>()
                            .unwrap()
                            .values_slice(),
                    ),
                    Datatype::Int32 => std::mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<PrimitiveBuilder<Int32Type>>()
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
        };
    }
}

unsafe impl Send for DynRecordBuilder {}
unsafe impl Sync for DynRecordBuilder {}

unsafe impl Send for DynRecordImmutableArrays {}
unsafe impl Sync for DynRecordImmutableArrays {}
