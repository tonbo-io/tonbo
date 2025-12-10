use std::{collections::BTreeSet, sync::Arc};

use arrow_schema::{Schema, SchemaRef};
use tonbo_predicate::{Operand, Predicate, PredicateNode};

use crate::{
    extractor::KeyExtractError,
    manifest::{SstEntry, TableSnapshot},
    mvcc::Timestamp,
};

/// Internal representation of a scan plan. Things included in the plan:
/// * predicate: the caller-supplied predicate used for pruning and residual evaluation
/// * range_set: cached primary-key ranges derived from the predicate for pruning
/// * immutable_memtable_idxes: which immutable memtables need to be scanned in execution phase
/// * ssts: level-ed sstable where entry contains the identifier and its corresponding pruning row
///   set result
/// * limit: the raw limit
/// * read_ts: snapshot/read timestamp
pub(crate) struct ScanPlan {
    pub(crate) _predicate: Predicate,
    pub(crate) immutable_indexes: Vec<usize>,
    pub(crate) residual_predicate: Option<Predicate>,
    pub(crate) projected_schema: Option<SchemaRef>,
    pub(crate) scan_schema: SchemaRef,
    pub(crate) limit: Option<usize>,
    pub(crate) read_ts: Timestamp,

    pub(crate) _snapshot: TableSnapshot,
}

pub(crate) fn projection_with_predicate(
    base_schema: &SchemaRef,
    projection: &SchemaRef,
    predicate: Option<&Predicate>,
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
    required: &BTreeSet<Arc<str>>,
) -> Result<SchemaRef, KeyExtractError> {
    if required.is_empty()
        || required.iter().all(|name| {
            projection
                .fields()
                .iter()
                .any(|field| field.name() == name.as_ref())
        })
    {
        return Ok(Arc::clone(projection));
    }

    let mut needed: BTreeSet<Arc<str>> = projection
        .fields()
        .iter()
        .map(|field| Arc::<str>::from(field.name().as_str()))
        .collect();
    needed.extend(required.iter().cloned());

    let mut fields = Vec::new();
    for field in base_schema.fields() {
        if needed.remove(field.name().as_str()) {
            fields.push(field.clone());
        }
    }

    if !needed.is_empty() {
        // TODO: add nested-column support once predicates can address nested fields.
        let missing = needed.iter().next().expect("missing column present");
        return Err(KeyExtractError::NoSuchField {
            name: missing.to_string(),
        });
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn collect_predicate_columns(predicate: &Predicate, out: &mut BTreeSet<Arc<str>>) {
    match predicate.kind() {
        PredicateNode::True => {}
        PredicateNode::Compare { left, right, .. } => {
            collect_operand_column(left, out);
            collect_operand_column(right, out);
        }
        PredicateNode::InList { expr, .. } | PredicateNode::IsNull { expr, .. } => {
            collect_operand_column(expr, out);
        }
        PredicateNode::Not(child) => collect_predicate_columns(child, out),
        PredicateNode::And(children) | PredicateNode::Or(children) => {
            for child in children {
                collect_predicate_columns(child, out);
            }
        }
    }
}

fn collect_operand_column(operand: &Operand, out: &mut BTreeSet<Arc<str>>) {
    if let Operand::Column(column) = operand {
        out.insert(Arc::clone(&column.name));
    }
}

impl ScanPlan {
    /// Access SST entries from the snapshot, grouped by compaction level.
    ///
    /// Returns all SST entries across all levels that should be scanned.
    /// Pruning based on key ranges or statistics will be added in future iterations.
    pub(crate) fn sst_entries(&self) -> impl Iterator<Item = &SstEntry> {
        self._snapshot
            .latest_version
            .as_ref()
            .map(|v| v.ssts())
            .unwrap_or(&[])
            .iter()
            .flatten()
    }
}
