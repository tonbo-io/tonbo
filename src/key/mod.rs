#![allow(dead_code)]
//! Zero-copy key projection scaffolding.
//!
//! This module introduces the skeleton for the zero-copy key design.
//! The goal is to keep hot-path key handling on borrow-based views
//! that reference Arrow buffers directly while providing an owned form only
//! where durability requires it.
mod heap_size;
mod owned;
mod range;
mod row;
mod ts;

pub use heap_size::KeyHeapSize;
pub use owned::KeyOwned;
pub(crate) use range::range_set_overlaps_bounds;
pub use range::{KeyRange, RangeSet};
pub use row::{KeyRow, KeyRowError};
pub use ts::{KeyTsOwned, KeyTsViewRaw};
