//! Generic DB that dispatches between compile-time typed and runtime schema modes via types.

use std::{collections::HashMap, marker::PhantomData, time::Instant};

use typed_arrow::{arrow_array::RecordBatch, arrow_schema::SchemaRef};

use crate::{
    inmem::{
        immutable::{
            arrays::{ImmutableArrays, ImmutableArraysBuilder},
            memtable::{ImmutableMemTable, segment_from_arrays, segment_from_batch_with_extractor},
        },
        mutable::{
            KeyHeapSize, MutableLayout,
            memtable::{DynLayout, DynRowScan, RowScan, TypedLayout},
        },
        policy::{SealDecision, SealPolicy, StatsProvider},
    },
    record::{
        Record,
        extract::{DynKeyExtractor, KeyDyn, KeyExtractError},
    },
    scan::RangeSet,
};

/// A DB parametrized by a mode `M` that defines key, payload and insert interface.
pub struct DB<M: Mode> {
    mode: M,
    mem: M::Mutable,
    // Immutable in-memory runs (frozen memtables) in recency order (oldest..newest)
    immutables: Vec<Immutable<M>>,
    // Sealing policy and last seal timestamp
    policy: Box<dyn SealPolicy + Send + Sync>,
    last_seal_at: Option<Instant>,
}

/// Mode trait describing how to insert and what is stored.
pub trait Mode {
    /// Logical key type stored in the memtable.
    type Key: Ord;

    /// Storage type inside the unified immutable segment for this mode.
    type ImmLayout;

    /// Mutable store type for this mode (columnar, last-writer index).
    type Mutable: MutableLayout<Self::Key>;
}

/// Convenience alias for the immutable segment type of a `Mode`.
pub(crate) type Immutable<M> = ImmutableMemTable<<M as Mode>::Key, <M as Mode>::ImmLayout>;

/// Typed mode: `R` is a compile-time record implementing `Record`.
pub struct TypedMode<R: Record> {
    _phantom: PhantomData<R>,
}

impl<R: Record> Mode for TypedMode<R>
where
    R::Key: KeyHeapSize,
{
    type Key = R::Key;
    type ImmLayout = ImmutableArrays<R>;
    type Mutable = TypedMem<R>;
}

impl<R: Record> DB<TypedMode<R>>
where
    R::Key: KeyHeapSize,
{
    /// Create a DB in typed mode.
    pub fn new_typed() -> Self {
        Self {
            mem: TypedMem::<R>::new(),
            mode: TypedMode {
                _phantom: PhantomData,
            },
            immutables: Vec::new(),
            policy: crate::inmem::policy::default_policy(),
            last_seal_at: None,
        }
    }

    /// Insert a typed record (compile-time schema path).
    pub fn insert(&mut self, row: R) {
        self.mem.insert(row);
        self.maybe_seal_after_insert();
    }

    /// Seal the open typed buffer and attach it as an immutable segment.
    #[allow(dead_code)]
    pub(crate) fn seal_and_attach(&mut self) {
        if let Some(rows) = self.mem.seal_open() {
            let mut builder = ImmutableArraysBuilder::<R>::new(rows.len());
            for row in rows {
                builder.push_row(row);
            }
            let arrays = builder.finish();
            let seg = segment_from_arrays::<R>(arrays);
            self.immutables.push(seg);
        }
    }
}

impl<R> DB<TypedMode<R>>
where
    R: Record,
    R::Key: KeyHeapSize,
{
    /// Scan the typed mutable memtable over key ranges, yielding latest rows per key.
    pub fn scan_mutable_rows<'a>(
        &'a self,
        ranges: &'a RangeSet<R::Key>,
    ) -> impl Iterator<Item = &'a R> + 'a {
        self.mem.scan_rows(ranges)
    }
}

impl<R: Record> DB<TypedMode<R>>
where
    R::Key: KeyHeapSize,
{
    fn maybe_seal_after_insert(&mut self) {
        let since = self.last_seal_at.map(|t| t.elapsed());
        let stats = self.mem.build_stats(since);
        if let SealDecision::Seal(_reason) = self.policy.evaluate(&stats) {
            if let Some(rows) = self.mem.seal_open() {
                let mut builder = ImmutableArraysBuilder::<R>::new(rows.len());
                for row in rows {
                    builder.push_row(row);
                }
                let arrays = builder.finish();
                let seg = segment_from_arrays::<R>(arrays);
                self.immutables.push(seg);
            }
            self.last_seal_at = Some(Instant::now());
        }
    }
}

/// Dynamic mode: runtime schema + trait-object extractor produce keys and store dynamic rows.
///
/// Notes:
/// - Enforces schema equality per DB instance: inserting a `RecordBatch` with a different schema
///   returns an error. Create a new DB for a new schema.
/// - Payloads are stored as `typed_arrow_dyn::DynRow` by value; string/binary cells are copied when
///   materializing rows via `row_from_batch`.
pub struct DynMode {
    schema: SchemaRef,
    extractor: Box<dyn DynKeyExtractor>,
}

impl Mode for DynMode {
    type Key = KeyDyn;
    type ImmLayout = RecordBatch;
    type Mutable = DynMem;
}

impl DB<DynMode> {
    /// Create a DB in dynamic mode from `schema` and a trait-object extractor.
    pub fn new_dyn(
        schema: SchemaRef,
        extractor: Box<dyn DynKeyExtractor>,
    ) -> Result<Self, KeyExtractError> {
        extractor.validate_schema(&schema)?;
        Ok(Self {
            mem: DynMem::new(),
            mode: DynMode { schema, extractor },
            immutables: Vec::new(),
            policy: crate::inmem::policy::default_policy(),
            last_seal_at: None,
        })
    }

    /// Create a dynamic DB by specifying the key column index.
    ///
    /// Validates that the column exists and its Arrow data type is supported for keys,
    /// then constructs the appropriate extractor internally.
    pub fn new_dyn_with_key_col(
        schema: SchemaRef,
        key_col: usize,
    ) -> Result<Self, KeyExtractError> {
        let fields = schema.fields();
        if key_col >= fields.len() {
            return Err(KeyExtractError::ColumnOutOfBounds(key_col, fields.len()));
        }
        let dt = fields[key_col].data_type();
        let extractor = crate::record::extract::dyn_extractor_for_field(key_col, dt)?;
        Self::new_dyn(schema, extractor)
    }

    /// Create a dynamic DB by specifying the key field name.
    ///
    /// Looks up the column index by name and delegates to `new_dyn_with_key_col`.
    pub fn new_dyn_with_key_name(
        schema: SchemaRef,
        key_field: &str,
    ) -> Result<Self, KeyExtractError> {
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
        Self::new_dyn_with_key_col(schema, idx)
    }

    /// Create a dynamic DB by inspecting Arrow metadata to find the key field(s).
    ///
    /// Priority:
    /// - Field-level: fields with metadata `tonbo.key = "true"` (single) or numeric ordinals like
    ///   `"1"`, `"2"` for composite keys (lexicographic order by ordinal).
    /// - Schema-level fallback: schema metadata `tonbo.keys` as a single name (e.g., `"id"`) or a
    ///   JSON-like array of names (e.g., `"[\"user_id\",\"ts\"]"`).
    ///
    /// Returns an error if no key is defined, a referenced field is missing, or multiple
    /// field-level markers are present without numeric ordinals.
    pub fn new_dyn_from_metadata(schema: SchemaRef) -> Result<Self, KeyExtractError> {
        // Helpers
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
                return Self::new_dyn_with_key_col(schema, idx);
            }
            // Composite: require numeric ordinals for disambiguation
            if marks.iter().any(|(o, _)| o.is_none()) {
                return Err(KeyExtractError::NoSuchField {
                    name: "multiple tonbo.key markers require numeric ordinals".to_string(),
                });
            }
            marks.sort_by_key(|(o, _)| o.unwrap());
            let mut parts: Vec<Box<dyn DynKeyExtractor>> = Vec::with_capacity(marks.len());
            for (_, idx) in marks.into_iter() {
                let dt = fields[idx].data_type();
                let ex = crate::record::extract::dyn_extractor_for_field(idx, dt)?;
                parts.push(ex);
            }
            let extractor = Box::new(crate::record::extract::CompositeDynExtractor::new(parts))
                as Box<dyn DynKeyExtractor>;
            return Self::new_dyn(schema, extractor);
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
                let key_name = &names[0];
                let Some((idx, _)) = fields
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name() == key_name)
                else {
                    return Err(KeyExtractError::NoSuchField {
                        name: key_name.to_string(),
                    });
                };
                return Self::new_dyn_with_key_col(schema, idx);
            } else {
                // Composite schema-level
                let mut parts: Vec<Box<dyn DynKeyExtractor>> = Vec::with_capacity(names.len());
                for n in names.iter() {
                    let Some((idx, f)) = fields.iter().enumerate().find(|(_, f)| f.name() == n)
                    else {
                        return Err(KeyExtractError::NoSuchField { name: n.clone() });
                    };
                    let dt = f.data_type();
                    let ex = crate::record::extract::dyn_extractor_for_field(idx, dt)?;
                    parts.push(ex);
                }
                let extractor = Box::new(crate::record::extract::CompositeDynExtractor::new(parts))
                    as Box<dyn DynKeyExtractor>;
                return Self::new_dyn(schema, extractor);
            }
        }

        // Nothing found
        Err(KeyExtractError::NoSuchField {
            name: "<tonbo.key|tonbo.keys>".to_string(),
        })
    }

    /// Insert a dynamic `RecordBatch`; last writer wins per key.
    pub fn insert_batch(&mut self, batch: RecordBatch) -> Result<(), KeyExtractError> {
        if self.mode.schema.as_ref() != batch.schema().as_ref() {
            return Err(KeyExtractError::SchemaMismatch {
                expected: self.mode.schema.clone(),
                actual: batch.schema(),
            });
        }
        self.mem.insert_batch(self.mode.extractor.as_ref(), batch)?;
        self.maybe_seal_after_insert()?;
        Ok(())
    }

    fn maybe_seal_after_insert(&mut self) -> Result<(), KeyExtractError> {
        let since = self.last_seal_at.map(|t| t.elapsed());
        let stats = self.mem.build_stats(since);
        if let SealDecision::Seal(_reason) = self.policy.evaluate(&stats) {
            let batches = self.mem.take_attached_batches();
            for b in batches {
                let seg = segment_from_batch_with_extractor(b, self.mode.extractor.as_ref())?;
                self.immutables.push(seg);
            }
            self.last_seal_at = Some(Instant::now());
        }
        Ok(())
    }
}

impl DB<DynMode> {
    /// Scan the dynamic mutable memtable over key ranges, yielding owned dynamic rows.
    pub fn scan_mutable_rows<'a>(
        &'a self,
        ranges: &'a RangeSet<KeyDyn>,
    ) -> impl Iterator<Item = typed_arrow_dyn::DynRow> + 'a {
        self.mem.scan_rows(ranges)
    }
}

// Methods common to all modes
impl<M: Mode> DB<M> {
    /// Unified ingestion entry point using `Insertable<M>` implementors.
    pub fn ingest<I: Insertable<M>>(&mut self, input: I) -> Result<(), KeyExtractError> {
        input.insert_into(self)
    }

    /// Ingest many items implementing `Insertable<M>`.
    pub fn ingest_many<I>(&mut self, inputs: I) -> Result<(), KeyExtractError>
    where
        I: IntoIterator,
        I::Item: Insertable<M>,
    {
        for item in inputs {
            item.insert_into(self)?;
        }
        Ok(())
    }

    /// Approximate bytes used by keys in the mutable memtable.
    pub fn approx_mutable_bytes(&self) -> usize {
        <M::Mutable as MutableLayout<M::Key>>::approx_bytes(&self.mem)
    }

    /// Number of immutable segments attached to this DB (oldest..newest).
    pub fn num_immutable_segments(&self) -> usize {
        self.immutables.len()
    }

    // Key-only merged scans have been removed.
}

// Segment management (generic, zero-cost)
impl<M: Mode> DB<M> {
    #[allow(dead_code)]
    pub(crate) fn add_immutable(&mut self, seg: Immutable<M>) {
        self.immutables.push(seg);
    }

    // Key-only convenience helpers removed.

    /// Set or replace the sealing policy used by this DB.
    pub fn set_seal_policy(&mut self, policy: Box<dyn SealPolicy + Send + Sync>) {
        self.policy = policy;
    }
}

/// A unified ingestion interface implemented for different input types per mode.
///
/// - For typed mode, implemented for a single row `R: Record` and for iterators of `R`.
/// - For dynamic mode, implemented for `RecordBatch` and iterators of `RecordBatch`.
pub trait Insertable<M: Mode> {
    /// Insert this value into the provided `DB` in mode `M`.
    ///
    /// Returns `Ok(())` on success, or a `KeyExtractError` for dynamic mode
    /// schema/key extraction issues.
    fn insert_into(self, db: &mut DB<M>) -> Result<(), KeyExtractError>;
}

// Typed mode: single row
impl<R> Insertable<TypedMode<R>> for R
where
    R: Record,
    R::Key: KeyHeapSize,
{
    fn insert_into(self, db: &mut DB<TypedMode<R>>) -> Result<(), KeyExtractError> {
        db.insert(self);
        Ok(())
    }
}

// Typed mode: Vec of rows
impl<R> Insertable<TypedMode<R>> for Vec<R>
where
    R: Record,
    R::Key: KeyHeapSize,
{
    fn insert_into(self, db: &mut DB<TypedMode<R>>) -> Result<(), KeyExtractError> {
        for row in self.into_iter() {
            db.mem.insert(row);
        }
        Ok(())
    }
}

// Dynamic mode: single RecordBatch
impl Insertable<DynMode> for RecordBatch {
    fn insert_into(self, db: &mut DB<DynMode>) -> Result<(), KeyExtractError> {
        if db.mode.schema.as_ref() != self.schema().as_ref() {
            return Err(KeyExtractError::SchemaMismatch {
                expected: db.mode.schema.clone(),
                actual: self.schema(),
            });
        }
        db.mem.insert_batch(db.mode.extractor.as_ref(), self)?;
        db.maybe_seal_after_insert()?;
        Ok(())
    }
}

// Dynamic mode: Vec of RecordBatch
impl Insertable<DynMode> for Vec<RecordBatch> {
    fn insert_into(self, db: &mut DB<DynMode>) -> Result<(), KeyExtractError> {
        for batch in self.into_iter() {
            if db.mode.schema.as_ref() != batch.schema().as_ref() {
                return Err(KeyExtractError::SchemaMismatch {
                    expected: db.mode.schema.clone(),
                    actual: batch.schema(),
                });
            }
            db.mem.insert_batch(db.mode.extractor.as_ref(), batch)?;
            db.maybe_seal_after_insert()?;
        }
        Ok(())
    }
}

/// Opaque typed mutable store for `DB<TypedMode<R>>`.
///
/// This wraps the internal `TypedLayout<R>` to avoid exposing private types via
/// the public `Mode` trait while preserving performance and behavior.
pub struct TypedMem<R: Record>(pub(crate) TypedLayout<R>);

impl<R> TypedMem<R>
where
    R: Record,
    R::Key: KeyHeapSize,
{
    pub(crate) fn new() -> Self {
        Self(TypedLayout::<R>::new())
    }

    pub(crate) fn insert(&mut self, row: R) {
        self.0.insert(row)
    }

    pub(crate) fn seal_open(&mut self) -> Option<Vec<R>> {
        self.0.seal_open()
    }

    pub(crate) fn scan_rows<'t, 's>(&'t self, ranges: &'s RangeSet<R::Key>) -> RowScan<'t, 's, R>
    where
        't: 's,
    {
        self.0.scan_rows(ranges)
    }
}

impl<R> MutableLayout<<R as Record>::Key> for TypedMem<R>
where
    R: Record,
    R::Key: KeyHeapSize,
{
    fn approx_bytes(&self) -> usize {
        self.0.approx_bytes()
    }
}

impl<R> StatsProvider for TypedMem<R>
where
    R: Record,
    R::Key: KeyHeapSize,
{
    fn build_stats(
        &self,
        since_last_seal: Option<std::time::Duration>,
    ) -> crate::inmem::policy::MemStats {
        self.0.build_stats(since_last_seal)
    }
}

/// Opaque dynamic mutable store for `DB<DynMode>`.
///
/// This wraps the internal `DynLayout` to avoid exposing private types via the
/// public `Mode` trait while preserving performance and behavior.
pub struct DynMem(pub(crate) DynLayout);

impl DynMem {
    pub(crate) fn new() -> Self {
        Self(DynLayout::new())
    }

    pub(crate) fn insert_batch(
        &mut self,
        extractor: &dyn DynKeyExtractor,
        batch: RecordBatch,
    ) -> Result<(), KeyExtractError> {
        self.0.insert_batch(extractor, batch)
    }

    pub(crate) fn take_attached_batches(&mut self) -> Vec<RecordBatch> {
        self.0.take_attached_batches()
    }

    pub(crate) fn scan_rows<'t, 's>(&'t self, ranges: &'s RangeSet<KeyDyn>) -> DynRowScan<'t, 's> {
        self.0.scan_rows(ranges)
    }
}

impl MutableLayout<KeyDyn> for DynMem {
    fn approx_bytes(&self) -> usize {
        self.0.approx_bytes()
    }
}

impl StatsProvider for DynMem {
    fn build_stats(
        &self,
        since_last_seal: Option<std::time::Duration>,
    ) -> crate::inmem::policy::MemStats {
        self.0.build_stats(since_last_seal)
    }
}

#[cfg(test)]
mod tests {
    use typed_arrow::arrow_schema::{DataType, Field, Schema};
    use typed_arrow_dyn::{DynCell, DynRow};
    use typed_arrow_unified::SchemaLike;

    use super::*;
    use crate::inmem::policy::{
        BatchesThreshold, BytesThreshold, OpenRowsThreshold, ReplaceRatioPolicy,
    };

    #[derive(typed_arrow::Record, Clone, Debug)]
    #[record(field_macro = crate::key_field)]
    struct RowT {
        #[record(ext(key))]
        id: u32,
        v: i32,
    }

    #[test]
    fn typed_seal_on_bytes_threshold() {
        let mut db: DB<TypedMode<RowT>> = DB::new_typed();
        assert_eq!(db.num_immutable_segments(), 0);
        db.set_seal_policy(Box::new(BytesThreshold { limit: 1 }));
        db.insert(RowT { id: 1, v: 10 });
        assert_eq!(db.num_immutable_segments(), 1);
    }

    #[test]
    fn typed_seal_on_open_rows_threshold() {
        let mut db: DB<TypedMode<RowT>> = DB::new_typed();
        db.set_seal_policy(Box::new(OpenRowsThreshold { rows: 2 }));
        db.insert(RowT { id: 1, v: 10 });
        assert_eq!(db.num_immutable_segments(), 0);
        db.insert(RowT { id: 2, v: 20 });
        assert_eq!(db.num_immutable_segments(), 1);
    }

    #[test]
    fn typed_seal_on_replace_ratio() {
        let mut db: DB<TypedMode<RowT>> = DB::new_typed();
        db.set_seal_policy(Box::new(ReplaceRatioPolicy {
            min_ratio: 0.5,
            min_inserts: 1,
        }));
        db.insert(RowT { id: 42, v: 1 });
        assert_eq!(db.num_immutable_segments(), 0);
        db.insert(RowT { id: 42, v: 2 });
        assert_eq!(db.num_immutable_segments(), 1);
    }

    #[test]
    fn dynamic_seal_on_batches_threshold() {
        // Build a simple schema: id: Utf8 (key), v: Int32
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        // Build one batch with two rows
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("valid dyn rows");

        let mut db = DB::new_dyn_with_key_name(schema.clone(), "id").expect("schema ok");
        db.set_seal_policy(Box::new(BatchesThreshold { batches: 1 }));
        assert_eq!(db.num_immutable_segments(), 0);
        db.ingest(batch).expect("insert batch");
        assert_eq!(db.num_immutable_segments(), 1);
    }

    #[test]
    fn dynamic_new_from_metadata_field_marker() {
        use std::collections::HashMap;
        // Schema: mark id with field-level metadata tonbo.key = true
        let mut fm = HashMap::new();
        fm.insert("tonbo.key".to_string(), "true".to_string());
        let f_id = Field::new("id", DataType::Utf8, false).with_metadata(fm);
        let f_v = Field::new("v", DataType::Int32, false);
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]));
        let mut db = DB::new_dyn_from_metadata(schema.clone()).expect("metadata key");

        // Build one batch and insert to ensure extractor wired
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("valid dyn rows");
        db.ingest(batch).expect("insert via metadata");
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[test]
    fn dynamic_new_from_metadata_schema_level() {
        use std::collections::HashMap;
        let f_id = Field::new("id", DataType::Utf8, false);
        let f_v = Field::new("v", DataType::Int32, false);
        let mut sm = HashMap::new();
        sm.insert("tonbo.keys".to_string(), "id".to_string());
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]).with_metadata(sm));
        let mut db = DB::new_dyn_from_metadata(schema.clone()).expect("schema metadata key");

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("x".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("y".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("valid dyn rows");
        db.ingest(batch).expect("insert via metadata");
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[test]
    fn dynamic_new_from_metadata_conflicts_and_missing() {
        use std::collections::HashMap;
        // Conflict: two fields marked as key
        let mut fm1 = HashMap::new();
        fm1.insert("tonbo.key".to_string(), "true".to_string());
        let mut fm2 = HashMap::new();
        fm2.insert("tonbo.key".to_string(), "1".to_string());
        let f1 = Field::new("id1", DataType::Utf8, false).with_metadata(fm1);
        let f2 = Field::new("id2", DataType::Utf8, false).with_metadata(fm2);
        let schema_conflict = std::sync::Arc::new(Schema::new(vec![f1, f2]));
        assert!(DB::new_dyn_from_metadata(schema_conflict).is_err());

        // Missing: no markers at field or schema level
        let schema_missing = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        assert!(DB::new_dyn_from_metadata(schema_missing).is_err());
    }

    #[test]
    fn dynamic_composite_from_field_ordinals_and_scan() {
        use std::collections::HashMap;
        // Fields: id (Utf8, ord 1), ts (Int64, ord 2), v (Int32)
        let mut m1 = HashMap::new();
        m1.insert("tonbo.key".to_string(), "1".to_string());
        let mut m2 = HashMap::new();
        m2.insert("tonbo.key".to_string(), "2".to_string());
        let f_id = Field::new("id", DataType::Utf8, false).with_metadata(m1);
        let f_ts = Field::new("ts", DataType::Int64, false).with_metadata(m2);
        let f_v = Field::new("v", DataType::Int32, false);
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]));
        let mut db = DB::new_dyn_from_metadata(schema.clone()).expect("composite field metadata");

        let rows = vec![
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(10)),
                Some(DynCell::I32(1)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(5)),
                Some(DynCell::I32(2)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I64(1)),
                Some(DynCell::I32(3)),
            ]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("valid dyn rows");
        db.ingest(batch).expect("insert batch");

        use std::ops::Bound as B;
        let lo = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(5i64)]);
        let hi = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(10i64)]);
        let rs = RangeSet::from_ranges(vec![crate::scan::KeyRange::new(
            B::Included(lo),
            B::Included(hi),
        )]);
        let got: Vec<(String, i64)> = db
            .scan_mutable_rows(&rs)
            .map(|row| match (&row.0[0], &row.0[1]) {
                (
                    Some(typed_arrow_dyn::DynCell::Str(s)),
                    Some(typed_arrow_dyn::DynCell::I64(ts)),
                ) => (s.clone(), *ts),
                _ => panic!("unexpected row content"),
            })
            .collect();
        assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
    }

    #[test]
    fn dynamic_composite_from_schema_list_and_scan() {
        use std::collections::HashMap;
        let f_id = Field::new("id", DataType::Utf8, false);
        let f_ts = Field::new("ts", DataType::Int64, false);
        let f_v = Field::new("v", DataType::Int32, false);
        let mut sm = HashMap::new();
        sm.insert("tonbo.keys".to_string(), "[\"id\", \"ts\"]".to_string());
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]).with_metadata(sm));
        let mut db = DB::new_dyn_from_metadata(schema.clone()).expect("composite schema metadata");

        let rows = vec![
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(5)),
                Some(DynCell::I32(1)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(10)),
                Some(DynCell::I32(2)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I64(1)),
                Some(DynCell::I32(3)),
            ]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("valid dyn rows");
        db.ingest(batch).expect("insert batch");

        use std::ops::Bound as B;
        let lo = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(1i64)]);
        let hi = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(10i64)]);
        let rs = RangeSet::from_ranges(vec![crate::scan::KeyRange::new(
            B::Included(lo),
            B::Included(hi),
        )]);
        let got: Vec<(String, i64)> = db
            .scan_mutable_rows(&rs)
            .map(|row| match (&row.0[0], &row.0[1]) {
                (
                    Some(typed_arrow_dyn::DynCell::Str(s)),
                    Some(typed_arrow_dyn::DynCell::I64(ts)),
                ) => (s.clone(), *ts),
                _ => panic!("unexpected row content"),
            })
            .collect();
        assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
    }
}
// duplicates removed (moved above tests)
