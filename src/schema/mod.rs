pub mod dynamic;

use arrow::array::ArrayRef;

use crate::filter::FilterProvider;

pub trait Key: Ord {
    /// The reference type for this key (e.g., FooRef for Foo)
    /// This type should implement Borrow<Self::Ref> for efficient lookups
    type Ref<'r>: Ord + 'r
    where
        Self: 'r;

    /// Convert this owned key to its reference type
    fn as_ref(&self) -> Self::Ref<'_>;
}

pub trait Row: 'static {
    type Schema: Schema<Row = Self>;

    type ArrowArrays: ArrowArrays<Row = Self, Schema = Self::Schema>;

    fn to_arrow_array(&self) -> arrow::array::ArrayRef;
}

pub trait ArrowArrays {
    type Schema: Schema<ArrowArrays = Self>;

    type Row: Row<ArrowArrays = Self, Schema = Self::Schema>;

    fn as_record_batch(&self) -> arrow::record_batch::RecordBatch;

    fn from_rows<I>(rows: I, schema: &Self::Schema) -> Self
    where
        I: IntoIterator<Item = Self::Row>;

    /// Get the array at the given column offset
    fn get_array(&self, offset: usize) -> Option<ArrayRef>;

    /// Get the number of rows
    fn len(&self) -> usize;

    /// Check if there are no rows
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Extract a key reference from the arrays at the given row
    /// Uses the primary key indices from the schema to determine which columns form the key
    fn key_ref_at(
        &self,
        row: usize,
        schema: &Self::Schema,
    ) -> <<Self::Schema as Schema>::Key as Key>::Ref<'_>;
}

pub trait Column: FilterProvider {
    fn arrow_field(&self) -> arrow::datatypes::Field;
}

pub trait Schema {
    type Row: Row<Schema = Self>;

    type ArrowArrays: ArrowArrays<Schema = Self>;

    type Key: Key + 'static;

    fn arrow_schema(&self) -> arrow::datatypes::SchemaRef;

    fn column(&self, offset: usize) -> Option<&dyn Column>;

    /// Returns the column indices that compose the primary key
    /// For example, [0, 2] means columns 0 and 2 form a composite primary key
    fn primary_key_indices(&self) -> &[usize];
}
