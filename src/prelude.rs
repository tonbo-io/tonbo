//! Convenience re-exports for common Tonbo usage.
//!
//! # Usage
//!
//! ```rust,ignore
//! use tonbo::prelude::*;
//!
//! #[derive(Record)]
//! struct User {
//!     #[metadata(k = "tonbo.key", v = "true")]
//!     id: String,
//!     name: String,
//! }
//!
//! let db = DbBuilder::from_schema(User::schema())?
//!     .on_disk("/tmp/users")?.open().await?;
//! ```

#[cfg(feature = "typed-arrow")]
pub use typed_arrow::{Record, prelude::*, schema::SchemaMeta};

pub use crate::{
    CommitAckMode,
    db::{ColumnRef, DB, DbBuilder, Predicate, ScalarValue},
};
