pub mod arrays;

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema as ArrowSchema},
};

pub use self::arrays::DynamicArrays;
use crate::{
    filter::{Filter, FilterError, FilterOp, FilterProvider},
    schema::{Column, Key, Row, Schema},
    value::{Value, ValueRef},
};

/// A dynamically defined schema that can be constructed at runtime
#[derive(Debug, Clone)]
pub struct DynamicSchema {
    #[allow(dead_code)]
    name: String,
    fields: Vec<DynamicField>,
    primary_key_indices: Vec<usize>,
    field_name_to_offset: HashMap<String, usize>,
}

/// A dynamically defined field
#[derive(Debug, Clone)]
pub struct DynamicField {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub offset: usize,
}

impl DynamicSchema {
    /// Create a new dynamic schema with a given name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: Vec::new(),
            primary_key_indices: Vec::new(),
            field_name_to_offset: HashMap::new(),
        }
    }

    /// Add a field to the schema
    pub fn with_field(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        let offset = self.fields.len();

        self.field_name_to_offset.insert(name.clone(), offset);
        self.fields.push(DynamicField {
            name,
            data_type,
            is_nullable: false,
            is_primary_key: false,
            offset,
        });
        self
    }

    /// Mark a field as part of the primary key
    pub fn with_primary_key(mut self, field_name: impl Into<String>) -> Self {
        let field_name = field_name.into();
        if let Some(&offset) = self.field_name_to_offset.get(&field_name) {
            self.fields[offset].is_primary_key = true;
            if !self.primary_key_indices.contains(&offset) {
                self.primary_key_indices.push(offset);
            }
        } else {
            panic!("Field '{field_name}' not found in schema");
        }
        self
    }

    /// Get the offset of a field by name
    pub fn field_offset(&self, field_name: &str) -> Option<usize> {
        self.field_name_to_offset.get(field_name).copied()
    }

    /// Get a field by name
    pub fn field(&self, field_name: &str) -> Option<&DynamicField> {
        self.field_offset(field_name)
            .and_then(|offset| self.fields.get(offset))
    }

    /// Get the data type of a field
    pub fn field_data_type(&self, field_name: &str) -> Option<&DataType> {
        self.field(field_name).map(|f| &f.data_type)
    }

    /// Get all field names
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }
}

impl Schema for DynamicSchema {
    type Row = DynamicRow;
    type ArrowArrays = DynamicArrays;
    type Key = DynamicKey;

    fn arrow_schema(&self) -> arrow::datatypes::SchemaRef {
        let fields: Vec<Field> = self
            .fields
            .iter()
            .map(|f| Field::new(&f.name, f.data_type.clone(), f.is_nullable))
            .collect();
        Arc::new(ArrowSchema::new(fields))
    }

    fn column(&self, offset: usize) -> Option<&dyn Column> {
        self.fields.get(offset).map(|field| field as &dyn Column)
    }

    fn primary_key_indices(&self) -> &[usize] {
        &self.primary_key_indices
    }
}

impl Column for DynamicField {
    fn arrow_field(&self) -> Field {
        Field::new(&self.name, self.data_type.clone(), self.is_nullable)
    }
}

impl FilterProvider for DynamicField {
    fn eq(&self, value: Value) -> Result<Filter, FilterError> {
        validate_value_type(&value, &self.data_type)?;
        Ok(Filter::new(self.offset, FilterOp::Eq, value))
    }

    fn neq(&self, value: Value) -> Result<Filter, FilterError> {
        validate_value_type(&value, &self.data_type)?;
        Ok(Filter::new(self.offset, FilterOp::Neq, value))
    }

    fn lt(&self, value: Value) -> Result<Filter, FilterError> {
        validate_value_type(&value, &self.data_type)?;
        Ok(Filter::new(self.offset, FilterOp::Lt, value))
    }

    fn le(&self, value: Value) -> Result<Filter, FilterError> {
        validate_value_type(&value, &self.data_type)?;
        Ok(Filter::new(self.offset, FilterOp::Le, value))
    }

    fn gt(&self, value: Value) -> Result<Filter, FilterError> {
        validate_value_type(&value, &self.data_type)?;
        Ok(Filter::new(self.offset, FilterOp::Gt, value))
    }

    fn ge(&self, value: Value) -> Result<Filter, FilterError> {
        validate_value_type(&value, &self.data_type)?;
        Ok(Filter::new(self.offset, FilterOp::Ge, value))
    }
}

/// A dynamically typed row that stores fields in a HashMap
#[derive(Debug, Clone)]
pub struct DynamicRow {
    fields: HashMap<String, Value>,
}

impl DynamicRow {
    /// Create a new empty dynamic row
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Set a field value
    pub fn set_field(&mut self, name: impl Into<String>, value: Value) {
        self.fields.insert(name.into(), value);
    }

    /// Get a field value
    pub fn get_field(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }

    /// Get all field names
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.keys().map(|s| s.as_str()).collect()
    }

    /// Create from a HashMap
    pub fn from_fields(fields: HashMap<String, Value>) -> Self {
        Self { fields }
    }

    /// Extract values in the order specified by the schema
    pub fn values_ordered(&self, field_names: &[&str]) -> Vec<Value> {
        field_names
            .iter()
            .map(|name| self.fields.get(*name).cloned().unwrap_or(Value::Null))
            .collect()
    }
}

impl Default for DynamicRow {
    fn default() -> Self {
        Self::new()
    }
}

/// A dynamic key that can represent composite primary keys
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DynamicKey {
    values: Vec<Value>,
}

impl DynamicKey {
    /// Create a new dynamic key from values
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Create a dynamic key from a row and primary key indices
    pub fn from_row(row: &DynamicRow, field_names: &[&str], primary_key_indices: &[usize]) -> Self {
        let all_values = row.values_ordered(field_names);
        let key_values: Vec<Value> = primary_key_indices
            .iter()
            .map(|&idx| all_values.get(idx).cloned().unwrap_or(Value::Null))
            .collect();
        Self::new(key_values)
    }
}

/// Reference type for DynamicKey
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DynamicKeyRef<'a> {
    values: Vec<ValueRef<'a>>,
}

impl<'a> DynamicKeyRef<'a> {
    /// Create a new dynamic key reference
    pub fn new(values: Vec<ValueRef<'a>>) -> Self {
        Self { values }
    }
}

impl Key for DynamicKey {
    type Ref<'r>
        = DynamicKeyRef<'r>
    where
        Self: 'r;

    fn as_ref(&self) -> Self::Ref<'_> {
        DynamicKeyRef::new(self.values.iter().map(ValueRef::from).collect())
    }
}

impl Row for DynamicRow {
    type Schema = crate::schema::dynamic::DynamicSchema;
    type ArrowArrays = crate::schema::dynamic::DynamicArrays;

    fn to_arrow_array(&self) -> ArrayRef {
        // This would convert a single row to an arrow array
        // For dynamic rows, this is complex and typically not needed
        todo!("to_arrow_array not implemented for DynamicRow")
    }
}

/// Validate that a Value matches the expected DataType
fn validate_value_type(value: &Value, expected_type: &DataType) -> Result<(), FilterError> {
    let actual_type = value.data_type();
    if !types_compatible(&actual_type, expected_type) {
        return Err(FilterError::TypeMismatch {
            expected: format!("{expected_type:?}"),
            actual: format!("{actual_type:?}"),
        });
    }
    Ok(())
}

/// Check if two DataTypes are compatible for filtering
fn types_compatible(actual: &DataType, expected: &DataType) -> bool {
    match (actual, expected) {
        (DataType::Null, _) => true, // Null is compatible with any type
        (a, e) => a == e,
    }
}
