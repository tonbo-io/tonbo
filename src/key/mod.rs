#![allow(dead_code)]
//! Zero-copy key projection scaffolding.
//!
//! This module introduces the skeleton for the zero-copy key design.
//! The goal is to keep hot-path key handling on borrow-based views
//! that reference Arrow buffers directly while providing an owned form only
//! where durability requires it.
mod owned;
mod raw;
mod view;

pub use owned::{KeyComponentOwned, KeyOwned};
pub use raw::{KeyComponentRaw, KeyViewRaw, SlicePtr};
pub use view::KeyView;
