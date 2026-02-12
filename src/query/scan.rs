use std::{
    collections::BTreeSet,
    ops::{Bound, Range},
    sync::Arc,
};

use arrow_schema::{DataType, Schema, SchemaRef};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{RowSelection, RowSelector},
    },
    file::metadata::ParquetMetaData,
};
use thiserror::Error;

use crate::{
    extractor::KeyExtractError,
    key::KeyOwned,
    manifest::{SstEntry, TableSnapshot},
    mvcc::Timestamp,
    query::{Expr, KeyPredicateValue, ScalarValue},
};

/// Selection information for a single SSTable scan.
#[derive(Clone, Debug)]
pub(crate) struct SstSelection {
    /// Row groups to include. None means all row groups.
    pub(crate) row_groups: Option<Vec<usize>>,
    /// Planned row set for this SST (per-source row id space).
    pub(crate) row_set: RowSet,
    /// Cached Parquet metadata loaded at plan time.
    pub(crate) metadata: Arc<ParquetMetaData>,
    /// Projection mask for required columns.
    pub(crate) projection: ProjectionMask,
    /// Arrow schema for the projected data stream.
    pub(crate) projected_schema: SchemaRef,
    /// Optional delete sidecar metadata and projection info.
    pub(crate) delete_selection: Option<DeleteSelection>,
}

/// Selection information for a delete sidecar scan.
#[derive(Clone, Debug)]
pub(crate) struct DeleteSelection {
    /// Cached Parquet metadata loaded at plan time.
    pub(crate) metadata: Arc<ParquetMetaData>,
    /// Projection mask for required columns.
    pub(crate) projection: ProjectionMask,
}

/// Placeholder for future key-range selections.
#[derive(Clone, Debug)]
pub(crate) struct KeyRangeSelection {
    /// Lower bound (inclusive/exclusive/unbounded).
    pub(crate) start: Bound<KeyOwned>,
    /// Upper bound (inclusive/exclusive/unbounded).
    pub(crate) end: Bound<KeyOwned>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct KeyBound {
    key: KeyOwned,
    inclusive: bool,
}

/// Key bounds derived from a predicate for source-level pruning.
///
/// `KeyBounds` is a coarse, conservative range used to decide whether an entire
/// source can be skipped before any row-level pruning is attempted. It is not a
/// row set itself; it only answers "can this source possibly contain matches?"
/// and drives `RowSet::none` vs `RowSet::all` decisions for memtables, and
/// pre-metadata pruning for SSTs.
///
/// Bounds are only derived for single-column primary keys. Multi-column keys
/// return `None`, leaving pruning to other mechanisms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeyBounds {
    lower: Option<KeyBound>,
    upper: Option<KeyBound>,
    empty: bool,
}

impl KeyBounds {
    fn empty() -> Self {
        Self {
            lower: None,
            upper: None,
            empty: true,
        }
    }

    fn with_lower(key: KeyOwned, inclusive: bool) -> Self {
        Self {
            lower: Some(KeyBound { key, inclusive }),
            upper: None,
            empty: false,
        }
    }

    fn with_upper(key: KeyOwned, inclusive: bool) -> Self {
        Self {
            lower: None,
            upper: Some(KeyBound { key, inclusive }),
            empty: false,
        }
    }

    fn with_range(lower: (KeyOwned, bool), upper: (KeyOwned, bool)) -> Self {
        let mut bounds = Self {
            lower: Some(KeyBound {
                key: lower.0,
                inclusive: lower.1,
            }),
            upper: Some(KeyBound {
                key: upper.0,
                inclusive: upper.1,
            }),
            empty: false,
        };
        bounds.normalize_empty();
        bounds
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.empty
    }

    fn normalize_empty(&mut self) {
        if let (Some(lower), Some(upper)) = (&self.lower, &self.upper) {
            match lower.key.cmp(&upper.key) {
                std::cmp::Ordering::Greater => {
                    self.empty = true;
                }
                std::cmp::Ordering::Equal => {
                    if !(lower.inclusive && upper.inclusive) {
                        self.empty = true;
                    }
                }
                std::cmp::Ordering::Less => {}
            }
        }
    }

    fn intersect(&self, other: &Self) -> Self {
        if self.empty || other.empty {
            return Self::empty();
        }
        let lower = match (&self.lower, &other.lower) {
            (Some(a), Some(b)) => match a.key.cmp(&b.key) {
                std::cmp::Ordering::Greater => Some(a.clone()),
                std::cmp::Ordering::Less => Some(b.clone()),
                std::cmp::Ordering::Equal => Some(KeyBound {
                    key: a.key.clone(),
                    inclusive: a.inclusive && b.inclusive,
                }),
            },
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        };
        let upper = match (&self.upper, &other.upper) {
            (Some(a), Some(b)) => match a.key.cmp(&b.key) {
                std::cmp::Ordering::Greater => Some(b.clone()),
                std::cmp::Ordering::Less => Some(a.clone()),
                std::cmp::Ordering::Equal => Some(KeyBound {
                    key: a.key.clone(),
                    inclusive: a.inclusive && b.inclusive,
                }),
            },
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        };
        let mut bounds = Self {
            lower,
            upper,
            empty: false,
        };
        bounds.normalize_empty();
        bounds
    }

    fn union_envelope(&self, other: &Self) -> Self {
        if self.empty {
            return other.clone();
        }
        if other.empty {
            return self.clone();
        }
        let lower = match (&self.lower, &other.lower) {
            (Some(a), Some(b)) => match a.key.cmp(&b.key) {
                std::cmp::Ordering::Greater => Some(b.clone()),
                std::cmp::Ordering::Less => Some(a.clone()),
                std::cmp::Ordering::Equal => Some(KeyBound {
                    key: a.key.clone(),
                    inclusive: a.inclusive || b.inclusive,
                }),
            },
            (None, _) | (_, None) => None,
        };
        let upper = match (&self.upper, &other.upper) {
            (Some(a), Some(b)) => match a.key.cmp(&b.key) {
                std::cmp::Ordering::Greater => Some(a.clone()),
                std::cmp::Ordering::Less => Some(b.clone()),
                std::cmp::Ordering::Equal => Some(KeyBound {
                    key: a.key.clone(),
                    inclusive: a.inclusive || b.inclusive,
                }),
            },
            (None, _) | (_, None) => None,
        };
        Self {
            lower,
            upper,
            empty: false,
        }
    }

    pub(crate) fn overlaps(&self, min_key: &KeyOwned, max_key: &KeyOwned) -> bool {
        if self.empty {
            return false;
        }
        if let Some(lower) = &self.lower {
            match max_key.cmp(&lower.key) {
                std::cmp::Ordering::Less => return false,
                std::cmp::Ordering::Equal if !lower.inclusive => return false,
                _ => {}
            }
        }
        if let Some(upper) = &self.upper {
            match min_key.cmp(&upper.key) {
                std::cmp::Ordering::Greater => return false,
                std::cmp::Ordering::Equal if !upper.inclusive => return false,
                _ => {}
            }
        }
        true
    }
}

/// Row selection wrapper for a single scan source.
///
/// A `RowSet` is scoped to a single source (memtable, immutable segment, or SST).
/// It stores the total number of rows in that source (the row-id universe),
/// plus a `RowSelection` describing which rows are selected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowSet {
    total_rows: usize,
    selection: RowSelection,
}

/// Errors raised when combining row sets with mismatched source lengths.
#[derive(Debug, Error)]
pub(crate) enum RowSetError {
    #[error("row set length mismatch: left {left} rows, right {right} rows")]
    LengthMismatch { left: usize, right: usize },
}

impl RowSet {
    /// Select all rows in a source with `total_rows` rows.
    pub(crate) fn all(total_rows: usize) -> Self {
        if total_rows == 0 {
            return Self::from_ranges(0, std::iter::empty());
        }
        Self::from_ranges(total_rows, std::iter::once(0..total_rows))
    }

    /// Select no rows in a source with `total_rows` rows.
    pub(crate) fn none(total_rows: usize) -> Self {
        Self::from_ranges(total_rows, std::iter::empty())
    }

    /// Build a row set from a precomputed row selection.
    ///
    /// Returns an error if the selection length does not match `total_rows`.
    pub(crate) fn from_row_selection(
        total_rows: usize,
        selection: RowSelection,
    ) -> Result<Self, RowSetError> {
        let implied_total: usize = selection.iter().map(|sel| sel.row_count).sum();
        if implied_total != total_rows {
            return Err(RowSetError::LengthMismatch {
                left: total_rows,
                right: implied_total,
            });
        }
        Ok(Self {
            total_rows,
            selection,
        })
    }

    /// Build a row set from consecutive row-id ranges.
    pub(crate) fn from_ranges<I>(total_rows: usize, ranges: I) -> Self
    where
        I: Iterator<Item = Range<usize>>,
    {
        let selection = RowSelection::from_consecutive_ranges(ranges, total_rows);
        Self {
            total_rows,
            selection,
        }
    }

    /// Total number of rows in the source (row-id universe size).
    pub(crate) fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// Number of selected rows.
    pub(crate) fn len(&self) -> usize {
        self.selection.row_count()
    }

    /// True if all rows are selected.
    pub(crate) fn is_full(&self) -> bool {
        self.len() == self.total_rows
    }

    /// True if no rows are selected.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn intersect(&self, other: &Self) -> Result<Self, RowSetError> {
        if self.total_rows != other.total_rows {
            return Err(RowSetError::LengthMismatch {
                left: self.total_rows,
                right: other.total_rows,
            });
        }
        Ok(Self {
            total_rows: self.total_rows,
            selection: self.selection.intersection(&other.selection),
        })
    }

    pub(crate) fn union(&self, other: &Self) -> Result<Self, RowSetError> {
        if self.total_rows != other.total_rows {
            return Err(RowSetError::LengthMismatch {
                left: self.total_rows,
                right: other.total_rows,
            });
        }
        Ok(Self {
            total_rows: self.total_rows,
            selection: self.selection.union(&other.selection),
        })
    }

    /// Iterate over selected row ids.
    pub(crate) fn iter(&self) -> RowSetIter<'_> {
        RowSetIter::new(&self.selection)
    }

    /// Access the underlying row selection.
    pub(crate) fn selection(&self) -> &RowSelection {
        &self.selection
    }

    /// Convert to `RowSelection`, returning `None` when full selection is implied.
    pub(crate) fn to_row_selection(&self) -> Option<RowSelection> {
        if self.is_full() {
            None
        } else {
            Some(self.selection.clone())
        }
    }
}

/// Iterator over selected row ids.
pub(crate) struct RowSetIter<'a> {
    selectors: Box<dyn Iterator<Item = &'a RowSelector> + 'a>,
    offset: usize,
    current: Option<Range<usize>>,
}

impl<'a> RowSetIter<'a> {
    fn new(selection: &'a RowSelection) -> Self {
        Self {
            selectors: Box::new(selection.iter()),
            offset: 0,
            current: None,
        }
    }
}

impl<'a> Iterator for RowSetIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(range) = self.current.as_mut() {
                if let Some(next) = range.next() {
                    return Some(next);
                }
                self.current = None;
            }
            let selector = self.selectors.next()?;
            if selector.row_count == 0 {
                continue;
            }
            if selector.skip {
                self.offset = self.offset.saturating_add(selector.row_count);
                continue;
            }
            let start = self.offset;
            let end = self.offset.saturating_add(selector.row_count);
            self.offset = end;
            self.current = Some(start..end);
        }
    }
}

/// Internal representation of a scan plan. Things included in the plan:
/// * pushdown_predicate: predicate portion safe for Parquet row filtering
/// * residual_predicate: predicate portion to evaluate after merge/materialization
/// * range_set: cached primary-key ranges derived from the predicate for pruning
/// * immutable_memtable_idxes: which immutable memtables need to be scanned in execution phase
/// * selections: per-source row selections (memtables default to all rows in this phase)
/// * row_sets: per-source row sets capturing planned pruning results
/// * ssts: level-ed sstable where entry contains the identifier and its corresponding selection
/// * limit: the raw limit
/// * read_ts: snapshot/read timestamp
pub(crate) struct ScanPlan {
    pub(crate) pushdown_predicate: Option<Expr>,
    pub(crate) immutable_indexes: Vec<usize>,
    pub(crate) mutable_row_set: RowSet,
    pub(crate) immutable_row_sets: Vec<RowSet>,
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

/// Derive primary-key bounds from a predicate for coarse pruning.
///
/// This extracts an approximate key range that can be applied to a source's
/// min/max key. If the bounds do not overlap, the source can be skipped.
/// Only single-column primary keys are currently supported; multi-column keys
/// return `None` (no pruning).
pub(crate) fn key_bounds_for_predicate(
    predicate: &Expr,
    key_schema: &SchemaRef,
) -> Option<KeyBounds> {
    let field = key_schema.fields().first()?;
    if key_schema.fields().len() != 1 {
        return None;
    }
    let key_column = field.name();
    bounds_from_expr(predicate, key_column, field.data_type())
}

fn scalar_to_key(value: &ScalarValue) -> Option<KeyOwned> {
    <KeyOwned as KeyPredicateValue>::from_scalar(value)
}

fn bounds_from_expr(expr: &Expr, key_column: &str, key_type: &DataType) -> Option<KeyBounds> {
    match expr {
        Expr::True => None,
        Expr::False => Some(KeyBounds::empty()),
        Expr::Cmp { column, op, value } if column == key_column => {
            let key = scalar_to_key(value)?;
            let bounds = match op {
                aisle::CmpOp::Eq => KeyBounds::with_range((key.clone(), true), (key, true)),
                aisle::CmpOp::NotEq => return None,
                aisle::CmpOp::Lt => KeyBounds::with_upper(key, false),
                aisle::CmpOp::LtEq => KeyBounds::with_upper(key, true),
                aisle::CmpOp::Gt => KeyBounds::with_lower(key, false),
                aisle::CmpOp::GtEq => KeyBounds::with_lower(key, true),
            };
            Some(bounds)
        }
        Expr::Between {
            column,
            low,
            high,
            inclusive,
        } if column == key_column => {
            let low_key = scalar_to_key(low)?;
            let high_key = scalar_to_key(high)?;
            Some(KeyBounds::with_range(
                (low_key, *inclusive),
                (high_key, *inclusive),
            ))
        }
        Expr::StartsWith { column, prefix } if column == key_column => {
            if !is_string_key(key_type) || prefix.is_empty() {
                return None;
            }
            let lower = KeyOwned::from(prefix.as_str());
            if let Some(next_prefix) = next_prefix_string(prefix) {
                let upper = KeyOwned::from(next_prefix.as_str());
                Some(KeyBounds::with_range((lower, true), (upper, false)))
            } else {
                Some(KeyBounds::with_lower(lower, true))
            }
        }
        Expr::InList { column, values } if column == key_column => {
            if values.is_empty() {
                return Some(KeyBounds::empty());
            }
            let mut keys = Vec::with_capacity(values.len());
            for value in values {
                let key = scalar_to_key(value)?;
                keys.push(key);
            }
            let min = keys.iter().min().cloned()?;
            let max = keys.iter().max().cloned()?;
            Some(KeyBounds::with_range((min, true), (max, true)))
        }
        Expr::And(children) => {
            let mut acc: Option<KeyBounds> = None;
            for child in children {
                if let Some(bounds) = bounds_from_expr(child, key_column, key_type) {
                    acc = Some(match acc {
                        Some(prev) => prev.intersect(&bounds),
                        None => bounds,
                    });
                }
            }
            acc
        }
        Expr::Or(children) => {
            let mut acc: Option<KeyBounds> = None;
            for child in children {
                let bounds = bounds_from_expr(child, key_column, key_type)?;
                acc = Some(match acc {
                    Some(prev) => prev.union_envelope(&bounds),
                    None => bounds,
                });
            }
            acc
        }
        Expr::Not(_) => None,
        _ => None,
    }
}

pub(crate) fn key_range_for_predicate(
    predicate: &Expr,
    key_schema: &SchemaRef,
) -> Option<KeyRangeSelection> {
    let bounds = key_bounds_for_predicate(predicate, key_schema)?;
    if bounds.is_empty() {
        return None;
    }
    Some(KeyRangeSelection {
        start: match bounds.lower.as_ref() {
            Some(bound) => {
                if bound.inclusive {
                    Bound::Included(bound.key.clone())
                } else {
                    Bound::Excluded(bound.key.clone())
                }
            }
            None => Bound::Unbounded,
        },
        end: match bounds.upper.as_ref() {
            Some(bound) => {
                if bound.inclusive {
                    Bound::Included(bound.key.clone())
                } else {
                    Bound::Excluded(bound.key.clone())
                }
            }
            None => Bound::Unbounded,
        },
    })
}

fn is_string_key(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

pub(crate) fn next_prefix_string(prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return None;
    }
    let mut chars: Vec<char> = prefix.chars().collect();
    for idx in (0..chars.len()).rev() {
        if let Some(next) = next_scalar_char(chars[idx]) {
            chars[idx] = next;
            chars.truncate(idx + 1);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

fn next_scalar_char(current: char) -> Option<char> {
    let mut next = u32::from(current).checked_add(1)?;
    while next <= u32::from(char::MAX) {
        if let Some(ch) = char::from_u32(next) {
            return Some(ch);
        }
        next += 1;
    }
    None
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

#[cfg(test)]
mod tests {
    use arrow_schema::Field;

    use super::*;

    #[test]
    fn row_set_all_and_none() {
        let all = RowSet::all(3);
        assert_eq!(all.total_rows(), 3);
        assert_eq!(all.len(), 3);
        assert!(all.is_full());
        assert!(!all.is_empty());
        let selected: Vec<usize> = all.iter().collect();
        assert_eq!(selected, vec![0, 1, 2]);
        assert!(all.to_row_selection().is_none());

        let none = RowSet::none(3);
        assert_eq!(none.total_rows(), 3);
        assert_eq!(none.len(), 0);
        assert!(none.is_empty());
        assert!(!none.is_full());
        let selection = none.to_row_selection().expect("row selection");
        assert_eq!(selection.row_count(), 0);
    }

    #[test]
    fn row_set_from_selection_validates_length() {
        let selection = RowSelection::from(vec![RowSelector::select(2)]);
        let err = RowSet::from_row_selection(3, selection).expect_err("length mismatch");
        match err {
            RowSetError::LengthMismatch { left, right } => {
                assert_eq!(left, 3);
                assert_eq!(right, 2);
            }
        }
    }

    #[test]
    fn next_prefix_string_skips_surrogate_gap() {
        let prefix = "\u{d7ff}";
        let expected = "\u{e000}".to_string();
        assert_eq!(next_prefix_string(prefix), Some(expected));
    }

    #[test]
    fn next_prefix_string_carries_past_max_scalar_tail() {
        let prefix = format!("a{}", char::MAX);
        assert_eq!(next_prefix_string(&prefix), Some("b".to_string()));
    }

    #[test]
    fn key_bounds_starts_with_keeps_upper_bound_across_unicode_gap() {
        let key_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let predicate = Expr::StartsWith {
            column: "id".to_string(),
            prefix: "\u{d7ff}".to_string(),
        };

        let bounds = key_bounds_for_predicate(&predicate, &key_schema).expect("bounds");
        assert!(!bounds.is_empty());
        let lower = bounds.lower.expect("lower");
        let upper = bounds.upper.expect("upper");
        assert_eq!(lower.key, KeyOwned::from("\u{d7ff}"));
        assert!(lower.inclusive);
        assert_eq!(upper.key, KeyOwned::from("\u{e000}"));
        assert!(!upper.inclusive);
    }
}
