use std::{collections::BTreeSet, sync::Arc};

use aisle::Expr;
use arrow_schema::{Schema, SchemaRef};
use parquet::arrow::arrow_reader::RowSelection;

use crate::{
    extractor::KeyExtractError,
    key::KeyOwned,
    manifest::{SstEntry, TableSnapshot},
    mvcc::Timestamp,
};

/// Selection information for a single SSTable scan.
#[derive(Clone, Debug)]
pub(crate) struct SstSelection {
    /// Row groups to include. Empty means all row groups.
    pub(crate) row_groups: Vec<usize>,
    /// Optional row-level selection within chosen row groups.
    pub(crate) row_selection: Option<RowSelection>,
}

impl SstSelection {
    pub(crate) fn all() -> Self {
        Self {
            row_groups: Vec::new(),
            row_selection: None,
        }
    }
}

/// Placeholder for future key-range selections.
#[derive(Clone, Debug)]
pub(crate) struct KeyRangeSelection {
    /// Inclusive lower bound.
    pub(crate) start: Option<KeyOwned>,
    /// Inclusive upper bound.
    pub(crate) end: Option<KeyOwned>,
}

/// Selection details for a scan source.
#[derive(Clone, Debug)]
pub(crate) enum ScanSelection {
    /// Scan all rows in the source.
    AllRows,
    /// Scan rows in a key range (not yet wired to pruning).
    KeyRange(KeyRangeSelection),
    /// Scan an SSTable with row-group and row selections.
    Sst(SstSelection),
}

/// SST entry paired with its scan selection.
#[derive(Clone, Debug)]
pub(crate) struct SstScanSelection {
    pub(crate) entry: SstEntry,
    pub(crate) selection: ScanSelection,
}

impl SstScanSelection {
    pub(crate) fn all(entry: SstEntry) -> Self {
        Self {
            entry,
            selection: ScanSelection::Sst(SstSelection::all()),
        }
    }
}

/// Internal representation of a scan plan. Things included in the plan:
/// * predicate: the caller-supplied predicate used for pruning and residual evaluation
/// * range_set: cached primary-key ranges derived from the predicate for pruning
/// * immutable_memtable_idxes: which immutable memtables need to be scanned in execution phase
/// * selections: per-source row selections (memtables default to all rows in this phase)
/// * ssts: level-ed sstable where entry contains the identifier and its corresponding selection
/// * limit: the raw limit
/// * read_ts: snapshot/read timestamp
pub(crate) struct ScanPlan {
    pub(crate) _predicate: Expr,
    pub(crate) immutable_indexes: Vec<usize>,
    pub(crate) mutable_selection: ScanSelection,
    pub(crate) immutable_selection: ScanSelection,
    pub(crate) sst_selections: Vec<SstScanSelection>,
    pub(crate) residual_predicate: Option<Expr>,
    pub(crate) projected_schema: Option<SchemaRef>,
    pub(crate) scan_schema: SchemaRef,
    pub(crate) limit: Option<usize>,
    pub(crate) read_ts: Timestamp,

    pub(crate) _snapshot: TableSnapshot,
}

pub(crate) fn projection_with_predicate(
    base_schema: &SchemaRef,
    projection: &SchemaRef,
    predicate: Option<&Expr>,
) -> Result<SchemaRef, KeyExtractError> {
    let mut required = BTreeSet::new();
    if let Some(pred) = predicate {
        collect_predicate_columns(pred, &mut required);
    }
    extend_projection_schema(base_schema, projection, &required)
}

fn extend_projection_schema(
    base_schema: &SchemaRef,
    projection: &SchemaRef,
    required: &BTreeSet<String>,
) -> Result<SchemaRef, KeyExtractError> {
    if required.is_empty()
        || required.iter().all(|name| {
            projection
                .fields()
                .iter()
                .any(|field| field.name() == name.as_str())
        })
    {
        return Ok(Arc::clone(projection));
    }

    let mut needed: BTreeSet<String> = projection
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();
    needed.extend(required.iter().cloned());

    let mut fields = Vec::new();
    for field in base_schema.fields() {
        if needed.remove(field.name()) {
            fields.push(field.clone());
        }
    }

    if !needed.is_empty() {
        // TODO: add nested-column support once predicates can address nested fields.
        if let Some(missing) = needed.iter().next() {
            return Err(KeyExtractError::NoSuchField {
                name: missing.to_string(),
            });
        }
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn collect_predicate_columns(predicate: &Expr, out: &mut BTreeSet<String>) {
    match predicate {
        Expr::True | Expr::False => {}
        Expr::Cmp { column, .. }
        | Expr::Between { column, .. }
        | Expr::InList { column, .. }
        | Expr::BloomFilterEq { column, .. }
        | Expr::BloomFilterInList { column, .. }
        | Expr::StartsWith { column, .. }
        | Expr::IsNull { column, .. } => {
            out.insert(column.clone());
        }
        Expr::Not(child) => collect_predicate_columns(child, out),
        Expr::And(children) | Expr::Or(children) => {
            for child in children {
                collect_predicate_columns(child, out);
            }
        }
        _ => {}
    }
}

impl ScanPlan {
    /// Access SST selections from the snapshot, grouped by compaction level.
    ///
    /// Returns all SST entries across all levels that should be scanned.
    /// Pruning based on key ranges or statistics will be added in future iterations.
    pub(crate) fn sst_selections(&self) -> impl Iterator<Item = &SstScanSelection> {
        self.sst_selections.iter()
    }
}
