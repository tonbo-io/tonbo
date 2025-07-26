pub mod immutable;
pub(crate) mod mutable;

#[derive(Debug)]
pub enum WriteResult {
    Continue,
    NeedCompaction,
}
