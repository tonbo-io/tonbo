use arrow_schema::SchemaRef;

use super::DynModeConfig;
use crate::extractor::{self, KeyExtractError, KeyProjection};

impl DynModeConfig {
    /// Build a config from a key column index within `schema`.
    pub fn from_key_col(schema: SchemaRef, key_col: usize) -> Result<Self, KeyExtractError> {
        let fields = schema.fields();
        if key_col >= fields.len() {
            return Err(KeyExtractError::ColumnOutOfBounds(key_col, fields.len()));
        }
        let dt = fields[key_col].data_type();
        let extractor = extractor::projection_for_field(key_col, dt)?;
        Self::new(schema, extractor)
    }

    /// Build a config from a key field name within `schema`.
    pub fn from_key_name(schema: SchemaRef, key_field: &str) -> Result<Self, KeyExtractError> {
        let fields = schema.fields();
        let Some((idx, _)) = fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == key_field)
        else {
            return Err(KeyExtractError::NoSuchField {
                name: key_field.to_string(),
            });
        };
        Self::from_key_col(schema, idx)
    }

    /// Build a config from schema metadata (`tonbo.key` markers).
    pub fn from_metadata(schema: SchemaRef) -> Result<Self, KeyExtractError> {
        use std::collections::HashMap;

        fn is_truthy(s: &str) -> bool {
            matches!(s, "true" | "TRUE" | "True" | "yes" | "YES" | "Yes")
        }
        fn parse_names_list(s: &str) -> Vec<String> {
            let t = s.trim();
            if t.starts_with('[') && t.ends_with(']') {
                let inner = &t[1..t.len() - 1];
                inner
                    .split(',')
                    .map(|p| p.trim().trim_matches('"').to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            } else {
                vec![t.trim_matches('"').to_string()]
            }
        }

        let fields = schema.fields();

        // 1) Field-level markers: collect (ord, idx) for any field with tonbo.key
        let mut marks: Vec<(Option<u32>, usize)> = Vec::new();
        for (i, f) in fields.iter().enumerate() {
            let md: &HashMap<String, String> = f.metadata();
            if let Some(v) = md.get("tonbo.key") {
                let v = v.trim();
                if let Ok(ord) = v.parse::<u32>() {
                    marks.push((Some(ord), i));
                } else if is_truthy(v) {
                    marks.push((None, i));
                }
            }
        }
        if !marks.is_empty() {
            if marks.len() == 1 {
                let idx = marks[0].1;
                return Self::from_key_col(schema, idx);
            }
            if marks.iter().any(|(o, _)| o.is_none()) {
                return Err(KeyExtractError::NoSuchField {
                    name: "multiple tonbo.key markers require numeric ordinals".to_string(),
                });
            }
            marks.sort_by_key(|(o, _)| o.unwrap());
            let mut parts: Vec<Box<dyn KeyProjection>> = Vec::with_capacity(marks.len());
            for (_, idx) in marks.into_iter() {
                let dt = fields[idx].data_type();
                let ex = extractor::projection_for_field(idx, dt)?;
                parts.push(ex);
            }
            let extractor =
                Box::new(extractor::CompositeProjection::new(parts)) as Box<dyn KeyProjection>;
            return Self::new(schema, extractor);
        }

        // 2) Schema-level fallback: tonbo.keys = "name" | "[\"a\",\"b\"]"
        let smd: &HashMap<String, String> = schema.metadata();
        if let Some(namev) = smd.get("tonbo.keys") {
            let names = parse_names_list(namev);
            if names.is_empty() {
                return Err(KeyExtractError::NoSuchField {
                    name: "tonbo.keys[]".to_string(),
                });
            }
            if names.len() == 1 {
                return Self::from_key_name(schema, &names[0]);
            }
            let mut parts: Vec<Box<dyn KeyProjection>> = Vec::with_capacity(names.len());
            for n in names.iter() {
                let Some((idx, f)) = fields.iter().enumerate().find(|(_, f)| f.name() == n) else {
                    return Err(KeyExtractError::NoSuchField { name: n.clone() });
                };
                let dt = f.data_type();
                let ex = extractor::projection_for_field(idx, dt)?;
                parts.push(ex);
            }
            let extractor =
                Box::new(extractor::CompositeProjection::new(parts)) as Box<dyn KeyProjection>;
            return Self::new(schema, extractor);
        }

        Err(KeyExtractError::NoSuchField {
            name: "<tonbo.key|tonbo.keys>".to_string(),
        })
    }
}
