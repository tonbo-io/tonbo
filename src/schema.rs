//! Schema builder utilities aligning with RFC 0009.

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use serde_json::json;

use crate::{
    extractor::{self, KeyExtractError},
    mode::DynModeConfig,
};

/// Builder for declaring primary keys against an Arrow schema.
///
/// The builder lets callers specify key columns programmatically while reusing
/// the same validation and extractor logic exercised by metadata-driven flows.
/// Optionally it can back-fill schema metadata (`tonbo.keys`) so downstream
/// tooling observes the same declaration.
#[derive(Clone)]
pub struct SchemaBuilder {
    schema: SchemaRef,
    key_parts: Vec<String>,
    write_metadata: bool,
}

impl SchemaBuilder {
    /// Start a builder from an Arrow schema reference.
    pub fn from_schema(schema: SchemaRef) -> Self {
        Self {
            schema,
            key_parts: Vec::new(),
            write_metadata: false,
        }
    }

    /// Declare a single-column primary key, replacing any prior selection.
    pub fn primary_key(mut self, field: impl Into<String>) -> Self {
        self.key_parts = vec![field.into()];
        self
    }

    /// Declare a composite key with fields in the provided order, replacing any prior selection.
    pub fn composite_key<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.key_parts = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Append a field to the key definition (useful for incremental configuration).
    pub fn add_key_part(mut self, field: impl Into<String>) -> Self {
        self.key_parts.push(field.into());
        self
    }

    /// Request that the builder writes the resulting key declaration back into schema metadata.
    pub fn with_metadata(mut self) -> Self {
        self.write_metadata = true;
        self
    }

    /// Finalise the builder, producing a `DynModeConfig` and optionally updated schema metadata.
    pub fn build(self) -> Result<DynModeConfig, KeyExtractError> {
        if self.key_parts.is_empty() {
            return Err(KeyExtractError::NoSuchField {
                name: "schema builder requires at least one key field".to_string(),
            });
        }

        let fields = self.schema.fields();
        let mut indices = Vec::with_capacity(self.key_parts.len());
        for name in &self.key_parts {
            let Some((idx, _)) = fields.iter().enumerate().find(|(_, f)| f.name() == name) else {
                return Err(KeyExtractError::NoSuchField { name: name.clone() });
            };
            indices.push(idx);
        }

        let schema = if self.write_metadata {
            let mut metadata = self.schema.metadata().clone();
            metadata.insert("tonbo.keys".to_string(), json!(self.key_parts).to_string());
            let field_refs = fields.iter().cloned().collect::<Vec<_>>();
            Arc::new(Schema::new_with_metadata(field_refs, metadata))
        } else {
            Arc::clone(&self.schema)
        };

        let extractor = extractor::projection_for_columns(Arc::clone(&schema), indices)?;

        DynModeConfig::new(schema, extractor)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::SchemaBuilder;

    #[test]
    fn primary_key_builder() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let config = SchemaBuilder::from_schema(Arc::clone(&schema))
            .primary_key("id")
            .build()
            .expect("builder should succeed");

        assert_eq!(config.schema.fields()[0].name(), "id");
    }

    #[test]
    fn composite_key_builder_sets_metadata() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));

        let config = SchemaBuilder::from_schema(Arc::clone(&schema))
            .composite_key(["pk", "ts"])
            .with_metadata()
            .build()
            .expect("builder should succeed");

        let md = config.schema.metadata();
        assert_eq!(md.get("tonbo.keys"), Some(&String::from("[\"pk\",\"ts\"]")));
    }

    #[test]
    fn missing_field_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));

        let err = match SchemaBuilder::from_schema(schema)
            .primary_key("missing")
            .build()
        {
            Ok(_) => panic!("builder should fail"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            crate::extractor::KeyExtractError::NoSuchField { .. }
        ));
    }
}
