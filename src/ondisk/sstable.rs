use std::{marker::PhantomData, ops::Bound};

use arrow::array::RecordBatch;
use futures_util::StreamExt;
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderBuilder, ArrowReaderOptions},
        arrow_writer::ArrowWriterOptions,
        async_reader::AsyncReader,
        AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};

use super::scan::SsTableScan;
use crate::{
    arrows::get_range_filter,
    executor::Executor,
    fs::AsyncFile,
    oracle::{timestamp::TimestampedRef, Timestamp},
    record::Record,
    stream::record_batch::RecordBatchEntry,
};

pub(crate) struct SsTable<R, E>
where
    R: Record,
    E: Executor,
{
    file: E::File,
    _marker: PhantomData<R>,
}

impl<R, E> SsTable<R, E>
where
    R: Record,
    E: Executor,
{
    pub(crate) fn open(file: E::File) -> Self {
        SsTable {
            file,
            _marker: PhantomData,
        }
    }

    fn create_writer(&mut self) -> AsyncArrowWriter<Compat<&mut dyn AsyncFile>> {
        // TODO: expose writer options
        let options = ArrowWriterOptions::new().with_properties(
            WriterProperties::builder()
                .set_created_by(concat!("seren version ", env!("CARGO_PKG_VERSION")).to_owned())
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
                .build(),
        );
        AsyncArrowWriter::try_new_with_options(
            (&mut self.file as &mut dyn AsyncFile).compat(),
            R::arrow_schema().clone(),
            options,
        )
        .expect("Failed to create writer")
    }

    async fn write(&mut self, record_batch: RecordBatch) -> parquet::errors::Result<()> {
        let mut writer = self.create_writer();
        writer.write(&record_batch).await?;

        if writer.in_progress_size() > (1 << 20) - 1 {
            writer.flush().await?;
        }

        writer.close().await?;
        Ok(())
    }

    async fn into_parquet_builder(
        self,
        limit: Option<usize>,
    ) -> parquet::errors::Result<ArrowReaderBuilder<AsyncReader<Compat<E::File>>>> {
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(
            self.file.compat(),
            ArrowReaderOptions::default().with_page_index(true),
        )
        .await?;
        if let Some(limit) = limit {
            builder = builder.with_limit(limit);
        }
        Ok(builder)
    }

    pub(crate) async fn get(
        self,
        key: &TimestampedRef<R::Key>,
    ) -> parquet::errors::Result<Option<RecordBatchEntry<R>>> {
        self.scan(
            (Bound::Included(key.value()), Bound::Unbounded),
            key.ts(),
            Some(1),
        )
        .await?
        .next()
        .await
        .transpose()
    }

    pub(crate) async fn scan<'scan>(
        self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        ts: Timestamp,
        limit: Option<usize>,
    ) -> Result<SsTableScan<R, E>, parquet::errors::ParquetError> {
        let builder = self.into_parquet_builder(limit).await?;

        let schema_descriptor = builder.metadata().file_metadata().schema_descr();
        let filter = unsafe { get_range_filter::<R>(schema_descriptor, range, ts) };

        Ok(SsTableScan::new(builder.with_row_filter(filter).build()?))
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    use super::SsTable;
    use crate::{
        executor::tokio::TokioExecutor,
        fs::FileProvider,
        oracle::timestamp::Timestamped,
        tests::{get_test_record_batch, Test},
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_sstable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let record_batch = get_test_record_batch::<TokioExecutor>().await;
        let file = TokioExecutor::open(&temp_dir.path().join("test.parquet"))
            .await
            .unwrap();
        let mut sstable = SsTable::<Test, TokioExecutor>::open(file);

        sstable.write(record_batch).await.unwrap();

        let key = Timestamped::new("hello".to_owned(), 1.into());

        dbg!(sstable.get(key.borrow()).await.unwrap().unwrap().get());
    }
}
