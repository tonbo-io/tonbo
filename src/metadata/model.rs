#![allow(unused)]

use sqlx::FromRow;

#[derive(Debug, Clone, PartialEq, sqlx::Type)]
#[sqlx(type_name = "file_type")]
pub(crate) enum FileType {
    Parquet,
    Wal,
    Log,
}

#[derive(Debug, FromRow)]
pub(crate) struct File {
    pub id: String,
    pub file_type: FileType,
    pub level: i16,
}

#[derive(Debug, FromRow)]
pub(crate) struct VersionSnapshotLeveledScope {
    pub row_index: i32,
    pub col_index: i32,
    pub file_id: String,
    pub size: i64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub scope_id: i32,
}

#[derive(Debug, FromRow)]
pub(crate) struct Scope {
    pub id: i32,
    pub file_id: String,
    pub size: i64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

#[derive(Debug, FromRow)]
pub(crate) struct ScopeWalFiles {
    pub file_id: String,
    pub scope_id: i32,
}

#[derive(Debug, FromRow)]
pub(crate) struct VersionSnapshot {
    pub id: i32,
    pub timestamp: i32,
    pub log_length: i32,
}
