use tonbo::{executor::Executor, record::Record};

mod compaction;
mod error;
mod metadata;
mod aws;

/// Trait for implmenting a cloud instance over different object storages
pub trait TonboCloud<R, E>
where R: Record, 
E: Executor
{   
    /// Creates a new Tonbo cloud instnace
    fn new();
    
    fn write(&self, records: impl ExactSizeIterator<Item = R>);

    fn read();

    /// Listens to new read requests from connections
    fn listen();

    // Updates metadata
    fn update_metadata();

    /// Creates SST and writes to object storage and local Tonbo instance
    fn flush();
}
