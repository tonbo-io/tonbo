use std::{any::Any, cell::RefCell, sync::Arc};

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBufferBuilder, PrimitiveBuilder,
        UInt32Builder,
    },
    datatypes::{Int16Type, Int8Type, Schema},
};

use super::{column::Column, record::DynRecord, record_ref::DynRecordRef, Datatype};
use crate::{
    inmem::immutable::{ArrowArrays, Builder},
    record::{Key, Record},
    timestamp::Timestamped,
};

#[allow(unused)]
pub struct DynRecordImmutableArrays {
    _null: std::sync::Arc<arrow::array::BooleanArray>,
    _ts: std::sync::Arc<arrow::array::UInt32Array>,
    columns: Vec<Column>,
    record_batch: arrow::record_batch::RecordBatch,
}

impl ArrowArrays for DynRecordImmutableArrays {
    type Record = DynRecord;

    type Builder = DynRecordBuilder;

    fn builder(schema: &Arc<Schema>, capacity: usize) -> Self::Builder {
        // let schema = dyn_schema();
        let mut builders: Vec<Box<dyn ArrayBuilder>> = vec![];
        let mut datatypes = vec![];
        for field in schema.fields().iter() {
            let datatype = field.data_type();
            datatypes.push(Datatype::from(datatype));
            match datatype {
                arrow::datatypes::DataType::Int8 => {
                    builders.push(Box::new(PrimitiveBuilder::<Int8Type>::with_capacity(
                        capacity,
                    )));
                }
                arrow::datatypes::DataType::Int16 => {
                    builders.push(Box::new(PrimitiveBuilder::<Int16Type>::with_capacity(
                        capacity,
                    )));
                }
                _ => todo!(),
            }
        }
        DynRecordBuilder {
            builders,
            datatypes,
            _null: arrow::array::BooleanBufferBuilder::new(capacity),
            _ts: arrow::array::UInt32Builder::with_capacity(capacity),
            schema: schema.clone(),
        }
    }

    #[allow(unused)]
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
                let value = match datatype {
                    Datatype::INT8 => {
                        Arc::new(col.value.downcast_ref::<i8>().copied()) as Arc<dyn Any>
                    }
                    Datatype::INT16 => {
                        Arc::new(col.value.downcast_ref::<i16>().copied()) as Arc<dyn Any>
                    }
                };
                columns.push(Column {
                    datatype,
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
        match row {
            Some(record_ref) => {
                for (builder, col) in self.builders.iter_mut().zip(record_ref.columns.iter()) {
                    let datatype = col.datatype;
                    match datatype {
                        Datatype::INT8 => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<Box<PrimitiveBuilder<Int8Type>>>()
                                .unwrap()
                                .append_value(*col.value.downcast_ref::<i8>().unwrap());
                        }
                        Datatype::INT16 => {
                            builder
                                .as_any_mut()
                                .downcast_mut::<Box<PrimitiveBuilder<Int16Type>>>()
                                .unwrap()
                                .append_value(*col.value.downcast_ref::<i16>().unwrap());
                        }
                    }
                }
            }
            None => {
                for (builder, datatype) in self.builders.iter().zip(self.datatypes.iter()) {
                    match datatype {
                        Datatype::INT8 => {
                            builder
                                .as_any()
                                .downcast_ref::<RefCell<PrimitiveBuilder<Int8Type>>>()
                                .unwrap()
                                .borrow_mut()
                                .append_null();
                        }
                        Datatype::INT16 => {
                            builder
                                .as_any()
                                .downcast_ref::<RefCell<PrimitiveBuilder<Int16Type>>>()
                                .unwrap()
                                .borrow_mut()
                                .append_null();
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
                    Datatype::INT8 => std::mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<PrimitiveBuilder<Int8Type>>()
                            .unwrap()
                            .values_slice(),
                    ),
                    Datatype::INT16 => std::mem::size_of_val(
                        builder
                            .as_any()
                            .downcast_ref::<PrimitiveBuilder<Int16Type>>()
                            .unwrap()
                            .values_slice(),
                    ),
                }
            })
    }

    fn finish(&mut self, indices: Option<&[usize]>) -> DynRecordImmutableArrays {
        let mut columns = vec![];
        for (builder, datatype) in self.builders.iter_mut().zip(self.datatypes.iter()) {
            match datatype {
                Datatype::INT8 => {
                    let value = builder
                        .as_any_mut()
                        .downcast_mut::<Box<PrimitiveBuilder<Int8Type>>>()
                        .unwrap()
                        .finish();
                    columns.push(Column {
                        datatype: Datatype::INT16,
                        value: Arc::new(value),
                        is_nullable: true,
                    });
                }
                Datatype::INT16 => {
                    let value = builder
                        .as_any_mut()
                        .downcast_mut::<Box<PrimitiveBuilder<Int16Type>>>()
                        .unwrap()
                        .finish();
                    columns.push(Column {
                        datatype: Datatype::INT16,
                        value: Arc::new(value),
                        is_nullable: true,
                    });
                }
            };
        }
        let _null = Arc::new(BooleanArray::new(self._null.finish(), None));
        let _ts = Arc::new(self._ts.finish());

        // pub type ArrayRef = Arc<dyn Array>;
        let mut array_refs = vec![Arc::clone(&_null) as ArrayRef, Arc::clone(&_ts) as ArrayRef];

        for builder in self.builders.iter_mut() {
            array_refs.push(builder.finish() as ArrayRef);
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

unsafe impl Send for DynRecordBuilder {}
unsafe impl Sync for DynRecordBuilder {}

unsafe impl Send for DynRecordImmutableArrays {}
unsafe impl Sync for DynRecordImmutableArrays {}
