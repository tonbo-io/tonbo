use std::{collections::BTreeSet, sync::Arc};

use arrow_schema::{Schema, SchemaRef};
use predicate::{Operand, Predicate, PredicateInner, PredicateLeaf, PredicateNode};

use crate::{
    extractor::KeyExtractError,
    key::{KeyOwned, RangeSet},
    mvcc::Timestamp,
};

/// Internal representation of a scan plan. Things included in the plan:
/// * ragnge_set: predicate yield to range set for the primary key
/// * immutable_memtable_idxes: which immutable memtables need to be scan in execution phase
/// * ssts: level-ed sstable where entry contains the identifier and its corresponding pruning row
///   set result
/// * predicate: the raw predicate filter
/// * limit: the raw limit
/// * read_rs: snapshot/read timestamp
pub struct ScanPlan {
    pub(crate) range_set: RangeSet<KeyOwned>,
    pub(crate) immutable_indexes: Vec<usize>,
    pub(crate) residual_predicate: Option<Predicate>,
    pub(crate) projected_schema: Option<SchemaRef>,
    pub(crate) scan_schema: SchemaRef,
    pub(crate) limit: Option<usize>,
    pub(crate) read_ts: Timestamp,
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
        PredicateNode::Leaf(leaf) => match leaf {
            PredicateLeaf::Compare { left, right, .. } => {
                collect_operand_column(left, out);
                collect_operand_column(right, out);
            }
            PredicateLeaf::InList { expr, .. } | PredicateLeaf::IsNull { expr, .. } => {
                collect_operand_column(expr, out);
            }
        },
        PredicateNode::Inner(inner) => match inner {
            PredicateInner::Not(child) => collect_predicate_columns(child, out),
            PredicateInner::And(children) | PredicateInner::Or(children) => {
                for child in children {
                    collect_predicate_columns(child, out);
                }
            }
        },
    }
}

fn collect_operand_column(operand: &Operand, out: &mut BTreeSet<Arc<str>>) {
    if let Operand::Column(column) = operand {
        out.insert(Arc::clone(&column.name));
    }
}
