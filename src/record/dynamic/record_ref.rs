use std::{mem, sync::Arc};

use arrow::{
    array::{Array, AsArray},
    datatypes::Schema as ArrowSchema,
};
use fusio::Write;
use fusio_log::Encode;

use crate::{
    magic::USER_COLUMN_OFFSET,
    record::{option::OptionRecordRef, DynRecord, Key, Record, RecordRef, Schema, ValueRef},
};

#[derive(Clone)]
pub struct DynRecordRef<'r> {
    pub columns: Vec<ValueRef<'r>>,
    // XXX: log encode should keep the same behavior
    pub primary_indices: Vec<usize>,
}

impl<'r> DynRecordRef<'r> {
    pub(crate) fn new(columns: Vec<ValueRef<'r>>, primary_indices: Vec<usize>) -> Self {
        Self {
            columns,
            primary_indices,
        }
    }
}

impl Encode for DynRecordRef<'_> {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        (self.columns.len() as u32).encode(writer).await?;
        (self.primary_indices.len() as u32).encode(writer).await?;
        for idx in self.primary_indices.iter() {
            (*idx as u32).encode(writer).await?;
        }
        for col in self.columns.iter() {
            col.encode(writer).await?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let mut size =
            2 * mem::size_of::<u32>() + self.primary_indices.len() * mem::size_of::<u32>();
        for col in self.columns.iter() {
            size += col.size();
        }
        size
    }
}

impl<'r> RecordRef<'r> for DynRecordRef<'r> {
    type Record = DynRecord;

    fn key(self) -> <<<Self::Record as Record>::Schema as Schema>::Key as Key>::Ref<'r> {
        // TODO: handle multiple primary keys
        self.columns
            .get(self.primary_indices[0])
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

        let pk_user_indices: Vec<usize> = metadata
            .get(crate::magic::PK_USER_INDICES)
            .map(|s| {
                s.split(',')
                    .filter(|p| !p.is_empty())
                    .filter_map(|p| p.parse::<usize>().ok())
                    .collect()
            })
            .expect("primary key user index must exist in schema metadata");
        let ts = record_batch
            .column(1)
            .as_primitive::<arrow::datatypes::UInt32Type>()
            .value(offset)
            .into();

        let mut columns = vec![];

        let schema = record_batch.schema();
        let flattened_fields = schema.fields();

        for (idx, field) in full_schema.fields().iter().enumerate().skip(2) {
            let batch_field = flattened_fields
                .iter()
                .enumerate()
                .find(|(_idx, f)| field.contains(f));
            if batch_field.is_none() {
                columns.push(ValueRef::Null);
                continue;
            }
            let array = record_batch.column(batch_field.unwrap().0);
            if array.is_null(offset) || !projection_mask.leaf_included(idx) {
                columns.push(ValueRef::Null);
            } else {
                // TODO: handle error
                columns.push(ValueRef::from_array_ref(array, offset).unwrap());
            }
        }

        let record = DynRecordRef {
            columns,
            primary_indices: pk_user_indices,
        };
        OptionRecordRef::new(ts, record, null)
    }

    fn projection(&mut self, projection_mask: &parquet::arrow::ProjectionMask) {
        for (idx, col) in self.columns.iter_mut().enumerate() {
            if !self.primary_indices.contains(&idx)
                && !projection_mask.leaf_included(idx + USER_COLUMN_OFFSET)
            {
                *col = ValueRef::Null;
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
    use parquet::arrow::{ArrowSchemaConverter, ProjectionMask};

    use crate::{
        dyn_schema, make_dyn_schema,
        record::{DynRecord, Record, RecordRef, Schema, TimeUnit, Value, ValueRef},
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
            [2]
        );
        let values = vec![
            Value::Boolean(true),
            Value::UInt32(7u32),
            Value::Float64(1.23),
            Value::Float32(1.23),
            Value::Null,
            Value::Float64(3.234),
            Value::Float64(13.234),
        ];
        let record = DynRecord::new(values, vec![2]);
        {
            // test project all
            let mut record_ref = record.as_record_ref();
            record_ref.projection(&ProjectionMask::all());
            let columns = record_ref.columns;
            assert_eq!(columns[0], ValueRef::Boolean(true));
            assert_eq!(columns[1], ValueRef::UInt32(7));
            assert_eq!(columns[2], ValueRef::Float64(1.23));
            assert_eq!(columns[3], ValueRef::Float32(1.23));
            assert_eq!(columns[4], ValueRef::Null);
            assert_eq!(columns[5], ValueRef::Float64(3.234));
            assert_eq!(columns[6], ValueRef::Float64(13.234));
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

            assert_eq!(columns[0], ValueRef::Null);
            assert_eq!(columns[1], ValueRef::Null);
            assert_eq!(columns[2], ValueRef::Float64(1.23));
            assert_eq!(columns[3], ValueRef::Null);
            assert_eq!(columns[3], ValueRef::Null);
            assert_eq!(columns[4], ValueRef::Null);
            assert_eq!(columns[5], ValueRef::Null);
            assert_eq!(columns[6], ValueRef::Null);
        }
    }

    #[test]
    fn test_string_projection() {
        let schema = dyn_schema!(
            ("_null", Boolean, false),
            ("ts", UInt32, false),
            ("id", Utf8, false),
            ("name", Utf8, false),
            ("email", Utf8, true),
            ("adress", Utf8, true),
            ("data", Binary, true),
            [2]
        );
        let values = vec![
            Value::Boolean(true),
            Value::UInt32(7u32),
            Value::String("abcd".to_string()),
            Value::String("Jack".to_string()),
            Value::String("abc@tonbo.io".to_string()),
            Value::Null,
            Value::Binary(b"hello,tonbo".to_vec()),
        ];
        let record = DynRecord::new(values, vec![2]);
        {
            // test project all
            let mut record_ref = record.as_record_ref();
            record_ref.projection(&ProjectionMask::all());
            let columns = record_ref.columns;
            assert_eq!(columns[0], ValueRef::Boolean(true));
            assert_eq!(columns[1], ValueRef::UInt32(7u32));
            assert_eq!(columns[2], ValueRef::String("abcd"));
            assert_eq!(columns[3], ValueRef::String("Jack"),);
            assert_eq!(columns[4], ValueRef::String("abc@tonbo.io"));
            assert_eq!(columns[6], ValueRef::Binary(b"hello,tonbo"),);
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
            assert_eq!(columns[0], ValueRef::Null);
            assert_eq!(columns[1], ValueRef::Null);
            assert_eq!(columns[2], ValueRef::String("abcd"));
            assert_eq!(columns[3], ValueRef::Null);
            assert_eq!(columns[4], ValueRef::Null);
            assert_eq!(columns[5], ValueRef::Null);
            assert_eq!(columns[6], ValueRef::Null);
        }
    }

    #[test]
    fn test_timestamp_projection() {
        let schema = make_dyn_schema!(
            ("_null", DataType::Boolean, false),
            ("_ts", DataType::UInt32, false),
            (
                "id",
                DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false
            ),
            (
                "ts1",
                DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false
            ),
            (
                "ts2",
                DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                true
            ),
            (
                "ts3",
                DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                true
            ),
            [2]
        );
        let values = vec![
            Value::Boolean(true),
            Value::UInt32(7u32),
            Value::Timestamp(1717507203412, TimeUnit::Millisecond),
            Value::Timestamp(1717507203432, TimeUnit::Millisecond),
            Value::Timestamp(1717507203442, TimeUnit::Millisecond),
            Value::Null,
        ];
        let record = DynRecord::new(values, vec![2]);
        {
            // test project all
            let mut record_ref = record.as_record_ref();
            record_ref.projection(&ProjectionMask::all());
            let columns = record_ref.columns;
            assert_eq!(
                columns[2],
                ValueRef::Timestamp(1717507203412, TimeUnit::Millisecond),
            );
            assert_eq!(
                columns[3],
                ValueRef::Timestamp(1717507203432, TimeUnit::Millisecond),
            );
            assert_eq!(
                columns[4],
                ValueRef::Timestamp(1717507203442, TimeUnit::Millisecond),
            );

            assert_eq!(columns[5], ValueRef::Null);
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
                columns[2],
                ValueRef::Timestamp(1717507203412, TimeUnit::Millisecond),
            );
            assert_eq!(columns[3], ValueRef::Null);
            assert_eq!(columns[4], ValueRef::Null);
            assert_eq!(columns[5], ValueRef::Null);
        }
    }
}
