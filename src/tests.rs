#[cfg(test)]
mod dynamic_schema_tests {
    use arrow::datatypes::DataType;

    use crate::{
        memtable::{ImmutableMemTable, MutableMemTable},
        schema::{
            dynamic::{DynamicArrays, DynamicRow, DynamicSchema},
            ArrowArrays, Schema,
        },
        value::Value,
        version::Timestamp,
    };

    #[test]
    fn test_dynamic_schema_creation() {
        let schema = DynamicSchema::new("users")
            .with_field("id", DataType::Int32)
            .with_field("timestamp", DataType::Int64)
            .with_field("name", DataType::Utf8)
            .with_field("age", DataType::Int32)
            .with_primary_key("id")
            .with_primary_key("timestamp");

        assert_eq!(schema.field_names().len(), 4);
        assert_eq!(schema.primary_key_indices(), &[0, 1]);
    }

    #[test]
    fn test_dynamic_row_operations() {
        let mut row = DynamicRow::new();
        row.set_field("id", Value::Int32(1));
        row.set_field("name", Value::String("Alice".to_string()));
        row.set_field("age", Value::Int32(30));

        assert_eq!(row.get_field("id"), Some(&Value::Int32(1)));
        assert_eq!(
            row.get_field("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(row.get_field("nonexistent"), None);
    }

    #[test]
    fn test_dynamic_memtable_transformation() {
        // Create schema
        let schema = DynamicSchema::new("users")
            .with_field("id", DataType::Int32)
            .with_field("timestamp", DataType::Int64)
            .with_field("name", DataType::Utf8)
            .with_field("age", DataType::Int32)
            .with_primary_key("id")
            .with_primary_key("timestamp");

        // Create mutable memtable
        let memtable = MutableMemTable::<DynamicRow>::new();

        // Create test data
        let mut row1 = DynamicRow::new();
        row1.set_field("id", Value::Int32(1));
        row1.set_field("timestamp", Value::Int64(1000));
        row1.set_field("name", Value::String("Alice".to_string()));
        row1.set_field("age", Value::Int32(30));

        let mut row2 = DynamicRow::new();
        row2.set_field("id", Value::Int32(2));
        row2.set_field("timestamp", Value::Int64(2000));
        row2.set_field("name", Value::String("Bob".to_string()));
        row2.set_field("age", Value::Int32(25));

        // Create keys
        let field_names = vec!["id", "timestamp", "name", "age"];
        let key1 = crate::schema::dynamic::DynamicKey::from_row(
            &row1,
            &field_names,
            schema.primary_key_indices(),
        );
        let key2 = crate::schema::dynamic::DynamicKey::from_row(
            &row2,
            &field_names,
            schema.primary_key_indices(),
        );

        // Insert rows
        memtable.insert(key1, Timestamp::from(1), row1);
        memtable.insert(key2, Timestamp::from(2), row2);

        // Transform to immutable
        let immutable: ImmutableMemTable<DynamicArrays> =
            ImmutableMemTable::from_mutable(memtable, schema);

        // Verify transformation
        assert_eq!(immutable.arrays().len(), 2);
    }

    #[test]
    fn test_dynamic_api_structure() {
        // This test demonstrates the API structure without requiring a runtime
        let schema = DynamicSchema::new("users")
            .with_field("id", DataType::Int32)
            .with_field("timestamp", DataType::Int64)
            .with_field("name", DataType::Utf8)
            .with_field("age", DataType::Int32)
            .with_primary_key("id")
            .with_primary_key("timestamp");

        // Verify schema structure
        assert_eq!(schema.field_names(), vec!["id", "timestamp", "name", "age"]);
        assert_eq!(schema.primary_key_indices(), &[0, 1]);

        // Test field access
        let id_field = schema.field("id").unwrap();
        assert_eq!(id_field.name, "id");
        assert_eq!(id_field.offset, 0);

        let age_field = schema.field("age").unwrap();
        assert_eq!(age_field.name, "age");
        assert_eq!(age_field.offset, 3);
    }
}

#[cfg(test)]
mod compile_time_schema_tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema as ArrowSchema},
        record_batch::RecordBatch,
    };

    use crate::memtable::{ImmutableMemTable, MutableMemTable};
    use crate::{
        filter::{Filter, FilterError, FilterOp, FilterProvider},
        schema::{ArrowArrays, Column, Key, Row, Schema},
        value::Value,
        version::Timestamp,
    };

    // Mock implementation of a composite key (user_id, tenant_id)
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct UserKey {
        user_id: String,
        tenant_id: i32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct UserKeyRef<'a> {
        user_id: &'a str,
        tenant_id: i32,
    }

    impl Key for UserKey {
        type Ref<'r>
            = UserKeyRef<'r>
        where
            Self: 'r;

        fn as_ref(&self) -> Self::Ref<'_> {
            UserKeyRef {
                user_id: &self.user_id,
                tenant_id: self.tenant_id,
            }
        }
    }

    // Mock row implementation
    #[derive(Debug, Clone)]
    struct UserRow {
        user_id: String,
        tenant_id: i32,
        name: String,
        age: i32,
    }

    impl Row for UserRow {
        type Schema = UserSchema;
        type ArrowArrays = UserArrays;

        fn to_arrow_array(&self) -> ArrayRef {
            // Convert a single row to arrow array - not used in our test
            todo!("to_arrow_array not needed for this test")
        }
    }

    // Compile-time column definitions using zero-sized types
    struct UserIdColumn;
    struct TenantIdColumn;
    struct NameColumn;
    struct AgeColumn;

    // Macro to implement Column and FilterProvider for each column type
    macro_rules! impl_column {
        ($column_type:ty, $name:expr, $data_type:expr, $offset:expr) => {
            impl Column for $column_type {
                fn arrow_field(&self) -> Field {
                    Field::new($name, $data_type, false)
                }
            }

            impl FilterProvider for $column_type {
                fn eq(&self, _value: Value) -> Result<Filter, FilterError> {
                    Ok(Filter::new($offset, FilterOp::Eq, _value))
                }
                fn neq(&self, _value: Value) -> Result<Filter, FilterError> {
                    Ok(Filter::new($offset, FilterOp::Neq, _value))
                }
                fn lt(&self, _value: Value) -> Result<Filter, FilterError> {
                    Ok(Filter::new($offset, FilterOp::Lt, _value))
                }
                fn le(&self, _value: Value) -> Result<Filter, FilterError> {
                    Ok(Filter::new($offset, FilterOp::Le, _value))
                }
                fn gt(&self, _value: Value) -> Result<Filter, FilterError> {
                    Ok(Filter::new($offset, FilterOp::Gt, _value))
                }
                fn ge(&self, _value: Value) -> Result<Filter, FilterError> {
                    Ok(Filter::new($offset, FilterOp::Ge, _value))
                }
            }
        };
    }

    // Apply the macro to each column
    impl_column!(UserIdColumn, "user_id", DataType::Utf8, 0);
    impl_column!(TenantIdColumn, "tenant_id", DataType::Int32, 1);
    impl_column!(NameColumn, "name", DataType::Utf8, 2);
    impl_column!(AgeColumn, "age", DataType::Int32, 3);

    // Mock schema implementation with compile-time columns
    struct UserSchema {
        // Compile-time column instances
        user_id: UserIdColumn,
        tenant_id: TenantIdColumn,
        name: NameColumn,
        age: AgeColumn,
    }

    impl UserSchema {
        fn new() -> Self {
            Self {
                user_id: UserIdColumn,
                tenant_id: TenantIdColumn,
                name: NameColumn,
                age: AgeColumn,
            }
        }

        // Compile-time constant for primary key indices
        const PRIMARY_KEY_INDICES: &'static [usize] = &[0, 1];
    }

    impl Schema for UserSchema {
        type Row = UserRow;
        type ArrowArrays = UserArrays;
        type Key = UserKey;

        fn arrow_schema(&self) -> arrow::datatypes::SchemaRef {
            // Build fields at compile time order
            let fields = vec![
                self.user_id.arrow_field(),
                self.tenant_id.arrow_field(),
                self.name.arrow_field(),
                self.age.arrow_field(),
            ];
            Arc::new(ArrowSchema::new(fields))
        }

        fn column(&self, offset: usize) -> Option<&dyn Column> {
            match offset {
                0 => Some(&self.user_id as &dyn Column),
                1 => Some(&self.tenant_id as &dyn Column),
                2 => Some(&self.name as &dyn Column),
                3 => Some(&self.age as &dyn Column),
                _ => None,
            }
        }

        fn primary_key_indices(&self) -> &[usize] {
            Self::PRIMARY_KEY_INDICES
        }
    }

    // Mock arrays implementation
    struct UserArrays {
        user_ids: Arc<StringArray>,
        tenant_ids: Arc<Int32Array>,
        names: Arc<StringArray>,
        ages: Arc<Int32Array>,
        len: usize,
    }

    impl ArrowArrays for UserArrays {
        type Schema = UserSchema;
        type Row = UserRow;

        fn as_record_batch(&self) -> RecordBatch {
            let schema = UserSchema::new();
            RecordBatch::try_new(
                schema.arrow_schema(),
                vec![
                    self.user_ids.clone() as ArrayRef,
                    self.tenant_ids.clone() as ArrayRef,
                    self.names.clone() as ArrayRef,
                    self.ages.clone() as ArrayRef,
                ],
            )
            .unwrap()
        }

        fn from_rows<I>(rows: I, _schema: &Self::Schema) -> Self
        where
            I: IntoIterator<Item = Self::Row>,
        {
            let rows: Vec<UserRow> = rows.into_iter().collect();

            let user_ids: StringArray = rows.iter().map(|r| Some(r.user_id.as_str())).collect();
            let tenant_ids: Int32Array = rows.iter().map(|r| Some(r.tenant_id)).collect();
            let names: StringArray = rows.iter().map(|r| Some(r.name.as_str())).collect();
            let ages: Int32Array = rows.iter().map(|r| Some(r.age)).collect();

            let len = rows.len();

            Self {
                user_ids: Arc::new(user_ids),
                tenant_ids: Arc::new(tenant_ids),
                names: Arc::new(names),
                ages: Arc::new(ages),
                len,
            }
        }

        fn get_array(&self, offset: usize) -> Option<ArrayRef> {
            match offset {
                0 => Some(self.user_ids.clone() as ArrayRef),
                1 => Some(self.tenant_ids.clone() as ArrayRef),
                2 => Some(self.names.clone() as ArrayRef),
                3 => Some(self.ages.clone() as ArrayRef),
                _ => None,
            }
        }

        fn len(&self) -> usize {
            self.len
        }

        fn key_ref_at(&self, row: usize, schema: &Self::Schema) -> UserKeyRef<'_> {
            let indices = schema.primary_key_indices();
            assert_eq!(indices, &[0, 1]);

            UserKeyRef {
                user_id: self.user_ids.value(row),
                tenant_id: self.tenant_ids.value(row),
            }
        }
    }

    #[test]
    fn test_mutable_to_immutable_transformation() {
        // Create a mutable memtable
        let memtable = MutableMemTable::<UserRow>::new();

        // Insert some test data
        let rows = vec![
            (
                UserKey {
                    user_id: "alice".to_string(),
                    tenant_id: 1,
                },
                UserRow {
                    user_id: "alice".to_string(),
                    tenant_id: 1,
                    name: "Alice".to_string(),
                    age: 30,
                },
            ),
            (
                UserKey {
                    user_id: "bob".to_string(),
                    tenant_id: 1,
                },
                UserRow {
                    user_id: "bob".to_string(),
                    tenant_id: 1,
                    name: "Bob".to_string(),
                    age: 25,
                },
            ),
            (
                UserKey {
                    user_id: "charlie".to_string(),
                    tenant_id: 2,
                },
                UserRow {
                    user_id: "charlie".to_string(),
                    tenant_id: 2,
                    name: "Charlie".to_string(),
                    age: 35,
                },
            ),
        ];

        let mut ts = 1;
        for (key, row) in rows {
            memtable.insert(key, Timestamp::from(ts), row);
            ts += 1;
        }

        // Also test deletion
        memtable.remove(
            UserKey {
                user_id: "bob".to_string(),
                tenant_id: 1,
            },
            Timestamp::from(4),
        );

        // Transform to immutable
        let schema = UserSchema::new();
        let immutable: ImmutableMemTable<UserArrays> =
            ImmutableMemTable::from_mutable(memtable, schema);

        // Verify the transformation
        // The immutable memtable should have 3 rows (alice, bob's first entry, charlie)
        // Bob's deletion entry should not create a row in the columnar storage
        assert_eq!(immutable.arrays().len(), 3);

        // Verify the arrays contain the correct data
        assert_eq!(immutable.arrays().user_ids.value(0), "alice");
        assert_eq!(immutable.arrays().user_ids.value(1), "bob");
        assert_eq!(immutable.arrays().user_ids.value(2), "charlie");

        assert_eq!(immutable.arrays().tenant_ids.value(0), 1);
        assert_eq!(immutable.arrays().tenant_ids.value(1), 1);
        assert_eq!(immutable.arrays().tenant_ids.value(2), 2);

        // Verify the index has the correct number of entries
        assert_eq!(immutable.index_len(), 3);

        println!("Test passed! Successfully transformed mutable memtable to immutable memtable.");
    }
}
