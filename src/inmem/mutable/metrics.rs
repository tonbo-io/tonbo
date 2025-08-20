#[derive(Clone, Debug, Default)]
pub(crate) struct MutableMemTableMetrics {
    pub entries: usize,
    pub inserts: u64,
    pub replaces: u64,
    pub approx_key_bytes: usize,
    pub entry_overhead: usize,
}
