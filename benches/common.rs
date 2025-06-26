use std::{
    collections::Bound,
    fs,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use async_stream::stream;
use fusio_dispatch::FsOptions;
use futures_core::Stream;
use futures_util::StreamExt;
use parquet::data_type::AsBytes;
use redb::TableDefinition;
use rocksdb::{Direction, IteratorMode, TransactionDB};
use tokio::fs::create_dir_all;
use tonbo::{
    executor::tokio::TokioExecutor, stream, transaction::TransactionEntry, DbOption, Projection,
};
use tonbo_macros::Record;

const RNG_SEED: u64 = 3;
pub(crate) const ITERATIONS: usize = 2;
pub(crate) const READ_TIMES: usize = 200;
pub(crate) const NUM_SCAN: usize = 200_000;
const STRING_SIZE: usize = 50;

#[allow(dead_code)]
pub(crate) const NUMBER_RECORD: usize = 40000000;

#[allow(dead_code)]
const X: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

type ItemKey = i32;
type ProjectionField = String;

// Tips: Tonbo only returns reference types
#[allow(dead_code)]
pub enum BenchResult<'a> {
    Ref(TransactionEntry<'a, Customer>),
    Owned(Box<Customer>),
}

#[allow(dead_code)]
pub enum ProjectionResult<'a> {
    // the entry is directly used to represent the field being projected.
    Ref(stream::Entry<'a, Customer>),
    Owned(ProjectionField),
}

#[derive(Record, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct Customer {
    #[record(primary_key)]
    pub c_custkey: i32,
    pub c_name: String,
    pub c_address: String,
    pub c_nationkey: i32,
    pub c_phone: String,
    pub c_acctbal: String,
    pub c_mktsegment: String,
    pub c_comment: String,
}

pub(crate) fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

pub(crate) fn make_rng_shards(shards: usize, elements: usize) -> Vec<fastrand::Rng> {
    let mut rngs = vec![];
    let elements_per_shard = elements / shards;
    for i in 0..shards {
        let mut rng = make_rng();
        for _ in 0..(i * elements_per_shard) {
            gen_record(&mut rng);
        }
        rngs.push(rng);
    }

    rngs
}

fn gen_string(rng: &mut fastrand::Rng, len: usize) -> String {
    let charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    let random_string: String = (0..len)
        .map(|_| {
            let idx = rng.usize(0..charset.len());
            charset.chars().nth(idx).unwrap()
        })
        .collect();
    random_string
}

pub(crate) fn gen_record(rng: &mut fastrand::Rng) -> Customer {
    Customer {
        c_custkey: rng.i32(..),
        c_name: gen_string(rng, STRING_SIZE),
        c_address: gen_string(rng, STRING_SIZE),
        c_nationkey: rng.i32(..),
        c_phone: gen_string(rng, STRING_SIZE),
        c_acctbal: gen_string(rng, STRING_SIZE),
        c_mktsegment: gen_string(rng, STRING_SIZE),
        c_comment: gen_string(rng, STRING_SIZE),
    }
}

#[allow(unused)]
pub(crate) fn read_tbl(file_path: impl AsRef<Path>) -> Box<dyn Iterator<Item = Customer>> {
    let file = File::open(file_path).expect("Cannot open file");
    let reader = BufReader::new(file);

    Box::new(reader.lines().filter_map(|line| {
        if let Ok(l) = line {
            let fields: Vec<&str> = l.split('|').collect();
            // 确保行数据符合预期
            if fields.len() > 7 {
                return Some(Customer {
                    c_custkey: fields[0].parse().unwrap(),
                    c_name: fields[1].to_string(),
                    c_address: fields[2].to_string(),
                    c_nationkey: fields[3].parse().unwrap(),
                    c_phone: fields[4].to_string(),
                    c_acctbal: fields[5].parse().unwrap(),
                    c_mktsegment: fields[6].to_string(),
                    c_comment: fields[7].to_string(),
                });
            }
        }
        None
    }))
}

pub(crate) fn gen_records(num_records: usize) -> Box<dyn Iterator<Item = Customer>> {
    let mut data = Vec::with_capacity(num_records);
    let mut rng = make_rng();
    for i in 0..num_records {
        data.push(gen_record(&mut rng));
    }
    Box::new(data.into_iter())
}

pub trait BenchDatabase {
    type W<'db>: BenchWriteTransaction
    where
        Self: 'db;
    type R<'db>: BenchReadTransaction
    where
        Self: 'db;

    fn db_type_name() -> &'static str;

    async fn write_transaction(&self) -> Self::W<'_>;

    async fn read_transaction(&self) -> Self::R<'_>;

    async fn build(path: impl AsRef<Path>) -> Self;
}

pub trait BenchWriteTransaction {
    type W<'txn>: BenchInserter
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_>;

    #[allow(clippy::result_unit_err)]
    async fn commit(self) -> Result<(), ()>;
}

pub trait BenchInserter {
    #[allow(clippy::result_unit_err)]
    fn insert(&mut self, record: Customer) -> Result<(), ()>;

    #[allow(clippy::result_unit_err)]
    fn remove(&mut self, key: ItemKey) -> Result<(), ()>;
}

pub trait BenchReadTransaction {
    type T<'txn>: BenchReader
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_>;
}

#[allow(clippy::len_without_is_empty)]
pub trait BenchReader {
    async fn get<'a>(&'a self, key: &'a ItemKey) -> Option<BenchResult>;

    fn range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = BenchResult> + 'a;

    fn projection_range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = ProjectionResult> + 'a;
}

pub struct TonboS3BenchDataBase {
    db: tonbo::DB<Customer, TokioExecutor>,
}

impl TonboS3BenchDataBase {
    #[allow(dead_code)]
    pub fn new(db: tonbo::DB<Customer, TokioExecutor>) -> Self {
        TonboS3BenchDataBase { db }
    }
}

impl BenchDatabase for TonboS3BenchDataBase {
    type W<'db>
        = TonboBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
        = TonboBenchReadTransaction<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "tonbo on s3"
    }

    async fn write_transaction(&self) -> Self::W<'_> {
        TonboBenchWriteTransaction {
            txn: self.db.transaction().await,
        }
    }

    async fn read_transaction(&self) -> Self::R<'_> {
        TonboBenchReadTransaction {
            txn: self.db.transaction().await,
        }
    }

    async fn build(path: impl AsRef<Path>) -> Self {
        create_dir_all(path.as_ref()).await.unwrap();

        let fs_options = FsOptions::S3 {
            bucket: "data".to_string(),
            credential: Some(fusio::remotes::aws::credential::AwsCredential {
                key_id: "user".to_string(),
                secret_key: "password".to_string(),
                token: None,
            }),
            endpoint: Some("http://localhost:9000".to_string()),
            sign_payload: None,
            checksum: None,
            region: None,
        };

        let path = fusio::path::Path::from_filesystem_path(path.as_ref()).unwrap();
        let option = DbOption::new(path.clone(), &CustomerSchema)
            .level_path(
                0,
                fusio::path::Path::from_url_path("/l0").unwrap(),
                fs_options.clone(),
                false,
            )
            .unwrap()
            .level_path(
                1,
                fusio::path::Path::from_url_path("/l1").unwrap(),
                fs_options.clone(),
                false,
            )
            .unwrap()
            .level_path(
                2,
                fusio::path::Path::from_url_path("/l2").unwrap(),
                fs_options.clone(),
                false,
            )
            .unwrap()
            .level_path(
                3,
                fusio::path::Path::from_url_path("/l3").unwrap(),
                fs_options.clone(),
                false,
            )
            .unwrap()
            .level_path(
                4,
                fusio::path::Path::from_url_path("/l4").unwrap(),
                fs_options.clone(),
                false,
            )
            .unwrap()
            .disable_wal();

        TonboS3BenchDataBase::new(
            tonbo::DB::new(option, TokioExecutor::current(), CustomerSchema)
                .await
                .unwrap(),
        )
    }
}

pub struct TonboBenchDataBase {
    db: tonbo::DB<Customer, TokioExecutor>,
}

impl TonboBenchDataBase {
    #[allow(dead_code)]
    pub fn new(db: tonbo::DB<Customer, TokioExecutor>) -> Self {
        TonboBenchDataBase { db }
    }
}

impl BenchDatabase for TonboBenchDataBase {
    type W<'db>
        = TonboBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
        = TonboBenchReadTransaction<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "tonbo"
    }

    async fn write_transaction(&self) -> Self::W<'_> {
        TonboBenchWriteTransaction {
            txn: self.db.transaction().await,
        }
    }

    async fn read_transaction(&self) -> Self::R<'_> {
        TonboBenchReadTransaction {
            txn: self.db.transaction().await,
        }
    }

    async fn build(path: impl AsRef<Path>) -> Self {
        create_dir_all(path.as_ref()).await.unwrap();

        let option = DbOption::new(
            fusio::path::Path::from_filesystem_path(path.as_ref()).unwrap(),
            &CustomerSchema,
        )
        .disable_wal();

        let db = tonbo::DB::new(option, TokioExecutor::current(), CustomerSchema)
            .await
            .unwrap();
        TonboBenchDataBase::new(db)
    }
}

pub struct TonboBenchReadTransaction<'a> {
    txn: tonbo::transaction::Transaction<'a, Customer>,
}

impl<'db> BenchReadTransaction for TonboBenchReadTransaction<'db> {
    type T<'txn>
        = TonboBenchReader<'db, 'txn>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        TonboBenchReader { txn: &self.txn }
    }
}

pub struct TonboBenchReader<'db, 'txn> {
    txn: &'txn tonbo::transaction::Transaction<'db, Customer>,
}

impl BenchReader for TonboBenchReader<'_, '_> {
    async fn get<'a>(&'a self, key: &'a ItemKey) -> Option<BenchResult> {
        self.txn
            .get(key, Projection::All)
            .await
            .unwrap()
            .map(BenchResult::Ref)
    }

    fn range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = BenchResult> + 'a {
        stream! {
            let mut stream = self.txn.scan(range).take().await.unwrap();

            while let Some(result) = stream.next().await {
                yield BenchResult::Ref(TransactionEntry::Stream(result.unwrap()));
            }
        }
    }

    fn projection_range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = ProjectionResult> + 'a {
        stream! {
            let mut stream = self.txn.scan(range).projection(&["c_name"]).take().await.unwrap();

            while let Some(entry) = stream.next().await.map(|result| result.unwrap()) {
                yield ProjectionResult::Ref(entry);
            }
        }
    }
}

pub struct TonboBenchWriteTransaction<'a> {
    txn: tonbo::transaction::Transaction<'a, Customer>,
}

impl<'db> BenchWriteTransaction for TonboBenchWriteTransaction<'db> {
    type W<'txn>
        = TonboBenchInserter<'db, 'txn>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        TonboBenchInserter { txn: &mut self.txn }
    }

    async fn commit(self) -> Result<(), ()> {
        self.txn.commit().await.unwrap();
        Ok(())
    }
}

pub struct TonboBenchInserter<'db, 'txn> {
    txn: &'txn mut tonbo::transaction::Transaction<'db, Customer>,
}

impl BenchInserter for TonboBenchInserter<'_, '_> {
    fn insert(&mut self, record: Customer) -> Result<(), ()> {
        self.txn.insert(record);
        Ok(())
    }

    fn remove(&mut self, key: ItemKey) -> Result<(), ()> {
        self.txn.remove(key);
        Ok(())
    }
}

pub struct RedbBenchDatabase {
    db: redb::Database,
}

impl RedbBenchDatabase {
    #[allow(dead_code)]
    pub fn new(db: redb::Database) -> Self {
        RedbBenchDatabase { db }
    }
}

impl BenchDatabase for RedbBenchDatabase {
    type W<'db>
        = RedbBenchWriteTransaction
    where
        Self: 'db;
    type R<'db>
        = RedbBenchReadTransaction
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "redb"
    }

    async fn write_transaction(&self) -> Self::W<'_> {
        let txn = self.db.begin_write().unwrap();
        RedbBenchWriteTransaction { txn }
    }

    async fn read_transaction(&self) -> Self::R<'_> {
        let txn = self.db.begin_read().unwrap();
        RedbBenchReadTransaction { txn }
    }

    async fn build(path: impl AsRef<Path>) -> Self {
        let db = redb::Database::builder()
            .set_cache_size(4 * 1024 * 1024 * 1024)
            .create(path)
            .unwrap();
        RedbBenchDatabase::new(db)
    }
}

pub struct RedbBenchReadTransaction {
    txn: redb::ReadTransaction,
}

impl BenchReadTransaction for RedbBenchReadTransaction {
    type T<'txn>
        = RedbBenchReader
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchReader { table }
    }
}

pub struct RedbBenchReader {
    table: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
}

impl BenchReader for RedbBenchReader {
    async fn get<'a>(&'a self, key: &'a ItemKey) -> Option<BenchResult> {
        self.table.get(key.as_bytes()).unwrap().map(|guard| {
            BenchResult::Owned(
                bincode::deserialize::<Customer>(guard.value())
                    .unwrap()
                    .into(),
            )
        })
    }

    fn range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = BenchResult> + 'a {
        let (lower, upper) = range;
        let iter = self
            .table
            .range::<&[u8]>((lower.map(ItemKey::as_bytes), upper.map(ItemKey::as_bytes)))
            .unwrap();

        stream! {
            for item in iter {
                let (_, v) = item.unwrap();
                yield BenchResult::Owned(bincode::deserialize(v.value()).unwrap());
            }
        }
    }

    fn projection_range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = ProjectionResult> + 'a {
        let (lower, upper) = range;
        let iter = self
            .table
            .range::<&[u8]>((lower.map(ItemKey::as_bytes), upper.map(ItemKey::as_bytes)))
            .unwrap();

        stream! {
            for item in iter {
                let (_, v) = item.unwrap();
                yield ProjectionResult::Owned(bincode::deserialize::<Customer>(v.value()).unwrap().c_name);
            }
        }
    }
}

pub struct RedbBenchWriteTransaction {
    txn: redb::WriteTransaction,
}

impl BenchWriteTransaction for RedbBenchWriteTransaction {
    type W<'txn>
        = RedbBenchInserter<'txn>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchInserter { table }
    }

    async fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RedbBenchInserter<'txn> {
    table: redb::Table<'txn, &'static [u8], &'static [u8]>,
}

impl BenchInserter for RedbBenchInserter<'_> {
    fn insert(&mut self, record: Customer) -> Result<(), ()> {
        self.table
            .insert(
                record.c_custkey.as_bytes(),
                bincode::serialize(&record).unwrap().as_bytes(),
            )
            .map(|_| ())
            .map_err(|_| ())
    }

    fn remove(&mut self, key: ItemKey) -> Result<(), ()> {
        self.table
            .remove(key.as_bytes())
            .map(|_| ())
            .map_err(|_| ())
    }
}

pub struct SledBenchDatabase {
    db: sled::Db,
    db_dir: PathBuf,
}

impl SledBenchDatabase {
    pub fn new(db: sled::Db, path: PathBuf) -> Self {
        SledBenchDatabase { db, db_dir: path }
    }
}

impl BenchDatabase for SledBenchDatabase {
    type W<'db>
        = SledBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
        = SledBenchReadTransaction<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "sled"
    }

    async fn write_transaction(&self) -> Self::W<'_> {
        SledBenchWriteTransaction {
            db: &self.db,
            db_dir: &self.db_dir,
        }
    }

    async fn read_transaction(&self) -> Self::R<'_> {
        SledBenchReadTransaction { db: &self.db }
    }

    async fn build(path: impl AsRef<Path>) -> Self {
        let path_buf = path.as_ref().to_path_buf();
        let db = sled::Config::new().path(path).open().unwrap();
        SledBenchDatabase::new(db, path_buf)
    }
}

pub struct SledBenchReadTransaction<'db> {
    db: &'db sled::Db,
}

impl BenchReadTransaction for SledBenchReadTransaction<'_> {
    type T<'txn>
        = SledBenchReader<'txn>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        SledBenchReader { db: &self.db }
    }
}

pub struct SledBenchReader<'db> {
    db: &'db sled::Db,
}

impl<'db> BenchReader for SledBenchReader<'db> {
    async fn get<'a>(&'a self, key: &'a ItemKey) -> Option<BenchResult> {
        self.db.get(key.as_bytes()).unwrap().map(|guard| {
            BenchResult::Owned(
                bincode::deserialize::<Customer>(guard.as_bytes())
                    .unwrap()
                    .into(),
            )
        })
    }

    fn range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = BenchResult> + 'a {
        let (lower, upper) = range;
        let iter = self.db.range::<&[u8], (Bound<&[u8]>, Bound<&[u8]>)>((
            lower.map(ItemKey::as_bytes),
            upper.map(ItemKey::as_bytes),
        ));

        stream! {
            for item in iter {
                let (_, v) = item.unwrap();
                yield BenchResult::Owned(bincode::deserialize(v.as_bytes()).unwrap());
            }
        }
    }

    fn projection_range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = ProjectionResult> + 'a {
        let (lower, upper) = range;
        let iter = self.db.range::<&[u8], (Bound<&[u8]>, Bound<&[u8]>)>((
            lower.map(ItemKey::as_bytes),
            upper.map(ItemKey::as_bytes),
        ));

        stream! {
            for item in iter {
                let (_, v) = item.unwrap();
                yield ProjectionResult::Owned(bincode::deserialize::<Customer>(v.as_bytes()).unwrap().c_name)
            }
        }
    }
}

pub struct SledBenchWriteTransaction<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl BenchWriteTransaction for SledBenchWriteTransaction<'_> {
    type W<'txn>
        = SledBenchInserter<'txn>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        SledBenchInserter { db: self.db }
    }

    async fn commit(self) -> Result<(), ()> {
        self.db.flush().unwrap();
        // Workaround for sled durability
        // Fsync all the files, because sled doesn't guarantee durability (it uses
        // sync_file_range()) See: https://github.com/spacejam/sled/issues/1351
        for entry in fs::read_dir(self.db_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.path().is_file() {
                let file = File::open(entry.path()).unwrap();
                let _ = file.sync_all();
            }
        }
        Ok(())
    }
}

pub struct SledBenchInserter<'a> {
    db: &'a sled::Db,
}

impl<'a> BenchInserter for SledBenchInserter<'a> {
    fn insert(&mut self, record: Customer) -> Result<(), ()> {
        self.db
            .insert(
                record.c_custkey.as_bytes(),
                bincode::serialize(&record).unwrap().as_bytes(),
            )
            .map(|_| ())
            .map_err(|_| ())
    }

    fn remove(&mut self, key: ItemKey) -> Result<(), ()> {
        self.db.remove(key.as_bytes()).map(|_| ()).map_err(|_| ())
    }
}

pub struct RocksdbBenchDatabase {
    db: TransactionDB,
}

impl RocksdbBenchDatabase {
    pub fn new(db: TransactionDB) -> Self {
        Self { db }
    }
}

impl BenchDatabase for RocksdbBenchDatabase {
    type W<'db>
        = RocksdbBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
        = RocksdbBenchReadTransaction<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "rocksdb"
    }

    async fn write_transaction(&self) -> Self::W<'_> {
        RocksdbBenchWriteTransaction {
            txn: self.db.transaction(),
        }
    }

    async fn read_transaction(&self) -> Self::R<'_> {
        RocksdbBenchReadTransaction {
            snapshot: self.db.snapshot(),
        }
    }

    async fn build(path: impl AsRef<Path>) -> Self {
        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&rocksdb::Cache::new_lru_cache(40 * 1_024 * 1_024));

        let mut opts = rocksdb::Options::default();
        opts.set_block_based_table_factory(&bb);
        opts.create_if_missing(true);

        let db = rocksdb::TransactionDB::open(&opts, &Default::default(), path).unwrap();
        RocksdbBenchDatabase::new(db)
    }
}

pub struct RocksdbBenchWriteTransaction<'a> {
    txn: rocksdb::Transaction<'a, TransactionDB>,
}

impl<'a> BenchWriteTransaction for RocksdbBenchWriteTransaction<'a> {
    type W<'txn>
        = RocksdbBenchInserter<'txn>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        RocksdbBenchInserter { txn: &self.txn }
    }

    async fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RocksdbBenchInserter<'a> {
    txn: &'a rocksdb::Transaction<'a, TransactionDB>,
}

impl BenchInserter for RocksdbBenchInserter<'_> {
    fn insert(&mut self, record: Customer) -> Result<(), ()> {
        self.txn
            .put(
                record.c_custkey.as_bytes(),
                bincode::serialize(&record).unwrap().as_bytes(),
            )
            .map(|_| ())
            .map_err(|_| ())
    }

    fn remove(&mut self, key: ItemKey) -> Result<(), ()> {
        self.txn.delete(key.as_bytes()).map(|_| ()).map_err(|_| ())
    }
}

pub struct RocksdbBenchReadTransaction<'db> {
    snapshot: rocksdb::SnapshotWithThreadMode<'db, TransactionDB>,
}

impl<'db> BenchReadTransaction for RocksdbBenchReadTransaction<'db> {
    type T<'txn>
        = RocksdbBenchReader<'db, 'txn>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        RocksdbBenchReader {
            snapshot: &self.snapshot,
        }
    }
}

pub struct RocksdbBenchReader<'db, 'txn> {
    snapshot: &'txn rocksdb::SnapshotWithThreadMode<'db, TransactionDB>,
}

impl<'db, 'txn> BenchReader for RocksdbBenchReader<'db, 'txn> {
    async fn get<'a>(&'a self, key: &'a ItemKey) -> Option<BenchResult> {
        self.snapshot.get(key.as_bytes()).unwrap().map(|bytes| {
            BenchResult::Owned(bincode::deserialize::<Customer>(&bytes).unwrap().into())
        })
    }

    fn range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = BenchResult> + 'a {
        fn bound_to_include(bound: Bound<&[u8]>) -> Option<&[u8]> {
            match bound {
                Bound::Included(bytes) | Bound::Excluded(bytes) => Some(bytes),
                Bound::Unbounded => None,
            }
        }

        let (lower, upper) = range;
        let lower = bound_to_include(lower.map(i32::as_bytes))
            .map(|bytes| IteratorMode::From(bytes, Direction::Forward))
            .unwrap_or(IteratorMode::Start);
        let upper = bound_to_include(upper.map(i32::as_bytes));

        let iter = self.snapshot.iterator(lower);

        stream! {
            for item in iter {
                let (key, v) = item.unwrap();
                if let Some(upper) = upper {
                    if upper.cmp(&key).is_lt() {
                        return;
                    }
                }
                yield BenchResult::Owned(bincode::deserialize(v.as_bytes()).unwrap());
            }
        }
    }

    fn projection_range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = ProjectionResult> + 'a {
        fn bound_to_include(bound: Bound<&[u8]>) -> Option<&[u8]> {
            match bound {
                Bound::Included(bytes) | Bound::Excluded(bytes) => Some(bytes),
                Bound::Unbounded => None,
            }
        }

        let (lower, upper) = range;
        let lower = bound_to_include(lower.map(i32::as_bytes))
            .map(|bytes| IteratorMode::From(bytes, Direction::Forward))
            .unwrap_or(IteratorMode::Start);
        let upper = bound_to_include(upper.map(i32::as_bytes));

        let iter = self.snapshot.iterator(lower);

        stream! {
            for item in iter {
                let (key, v) = item.unwrap();
                if let Some(upper) = upper {
                    if upper.cmp(&key).is_lt() {
                        return;
                    }
                }
                yield ProjectionResult::Owned(bincode::deserialize::<Customer>(v.as_bytes()).unwrap().c_name)
            }
        }
    }
}
