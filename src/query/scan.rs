use predicate::{DynRowSet, Predicate};
use typed_arrow_dyn::DynProjection;

use crate::{
    key::KeyTsViewRaw, mvcc::Timestamp, ondisk::sstable::SsTableDescriptor, scan::RangeSet,
};

/// Internal representation of a scan plan. Things included in the plan:
/// * ragnge_set: predicate yield to range set for the primary key
/// * immutable_memtable_idxes: which immutable memtables need to be scan in execution phase
/// * ssts: level-ed sstable where entry contains the identifier and its corresponding pruning row
///   set result
/// * predicate: the raw predicate filter
/// * limit: the raw limit
/// * read_rs: snapshot/read timestamp
pub struct ScanPlan<'t> {
    _range_set: RangeSet<KeyTsViewRaw>,
    _immutable_memtables_idxes: &'t [usize],
    _ssts: &'t [Vec<(&'t SsTableDescriptor, DynRowSet)>],
    _predicate: &'t Predicate,
    _projection: &'t DynProjection,
    _limit: Option<usize>,
    _read_ts: Timestamp,
}
