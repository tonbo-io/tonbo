# Tonbo

A work-in-progress embedded key-value database written in Rust, featuring:

- **Type-safe schema abstraction** with compile-time guarantees
- **Row-based and columnar storage** with Apache Arrow integration
- **Composite primary key support** for complex data models
- **Dynamic schema API** for runtime flexibility

## Usage Examples

### Compile-Time Type-Safe API (Planned)

```rust
use tonbo::{Db, Schema, Row, Key};

// Define your schema at compile time
struct UserSchema;

impl Schema for UserSchema {
    type Row = UserRow;
    type ArrowArrays = UserArrays;
    type Key = UserKey;
    
    fn arrow_schema(&self) -> arrow::datatypes::SchemaRef { 
        // ... Arrow schema definition
    }
    
    fn primary_key_indices(&self) -> &[usize] {
        &[0, 1] // Composite key: id + timestamp
    }
}

// Define your row type
struct UserRow {
    id: i32,
    timestamp: i64,
    name: String,
    age: i32,
}

// Define your key type
#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct UserKey {
    id: i32,
    timestamp: i64,
}

// Usage
let db = Db::<UserSchema>::new();

// Type-safe operations (planned API)
let results = db.transaction()
    .scan()
    .filter(|row| row.name == "Alice" && row.age < 30)
    .projection(|row| (row.id, row.name))
    .limit(10)
    .execute()
    .await?;
```

### Runtime Dynamic Schema API (Implemented)

```rust
use tonbo::schema::dynamic::{DynamicSchema, DynamicRow};
use arrow::datatypes::DataType;

// Create a schema at runtime
let schema = DynamicSchema::new("users")
    .with_field("id", DataType::Int32)
    .with_field("timestamp", DataType::Int64)  
    .with_field("name", DataType::Utf8)
    .with_field("age", DataType::Int32)
    .with_primary_key("id")        // Composite primary key
    .with_primary_key("timestamp");

// Create and populate a row
let mut row = DynamicRow::new();
row.set_field("id", Value::Int32(1));
row.set_field("timestamp", Value::Int64(1234567890));
row.set_field("name", Value::String("Alice".to_string()));
row.set_field("age", Value::Int32(25));

// Work with dynamic fields
let field = schema.field("name").unwrap();
let filter = field.eq(Value::String("Alice".to_string()))?;

// Example: Extract composite key from row
let key = DynamicKey::from_row(&row, &["id", "timestamp", "name", "age"], &[0, 1]);
```

### Working with Columnar Data

```rust
use tonbo::schema::{ArrowArrays, Schema};
use tonbo::schema::dynamic::DynamicArrays;

// Convert rows to columnar format
let rows = vec![row1, row2, row3];
let arrays = DynamicArrays::from_rows(rows, &schema);

// Access columnar data efficiently
let record_batch = arrays.as_record_batch();
let name_column = arrays.get_array(2); // Column 2 is "name"

// Extract key references without allocation
let key_ref = arrays.key_ref_at(0, &schema); // Key at row 0
```

## Current Implementation Status

✅ **Core Type System**: Schema, Row, Key, and ArrowArrays traits  
✅ **Dynamic Schema**: Runtime schema creation with field validation  
✅ **Composite Keys**: Support for multi-column primary keys  
✅ **Memory-Safe Access**: ValueRef type for zero-copy columnar data access  
✅ **Arrow Integration**: Full Apache Arrow array support  
🚧 **Storage Backend**: Memtable, SSTable, and WAL (in progress)  
🚧 **Query Execution**: Filter pushdown and projection (planned)  
🚧 **Transactions**: MVCC and snapshot isolation (planned)  
🚧 **Type-Safe API**: Compile-time schema definition (planned)  

## Architecture Highlights

- **Zero-Copy Design**: ValueRef allows accessing columnar data without allocation
- **Type-Safe Keys**: Primary keys are part of the schema, ensuring consistency
- **Flexible Storage**: Row-based for writes, columnar for reads
- **Extensible**: Easy to add new data types and storage backends