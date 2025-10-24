use std::{
    fs,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{executor::tokio::TokioExecutor, path::Path as FusioPath};
use tonbo::{
    record::extract::{dyn_extractor_for_field, KeyDyn},
    scan::RangeSet,
    wal::{WalConfig, WalExt},
    DB,
    mode::{DynMode, DynModeConfig},
};
use typed_arrow_dyn::DynCell;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovers_rows_across_restart() -> Result<(), Box<dyn std::error::Error>> {
    let wal_dir = std::env::temp_dir().join(format!(
        "tonbo-wal-e2e-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_nanos()
    ));
    fs::create_dir_all(&wal_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let extractor = dyn_extractor_for_field(0, &DataType::Utf8)?;
    let mode_config = DynModeConfig::new(schema.clone(), extractor)?;

    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::new(mode_config, Arc::clone(&executor))?;

    let mut wal_cfg = WalConfig::default();
    wal_cfg.dir = FusioPath::from_filesystem_path(&wal_dir)?;
    let recovery_cfg = wal_cfg.clone();

    db.enable_wal(wal_cfg)?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user-1", "user-2"])) as _,
            Arc::new(Int32Array::from(vec![10, 20])) as _,
        ],
    )?;
    db.ingest(batch).await?;

    db.disable_wal()?;
    drop(db);

    let extractor_recover = dyn_extractor_for_field(0, &DataType::Utf8)?;
    let mode_config_recover = DynModeConfig::new(schema.clone(), extractor_recover)?;
    let recovered: DB<DynMode, TokioExecutor> =
        DB::recover_with_wal(mode_config_recover, Arc::clone(&executor), recovery_cfg).await?;

    let ranges = RangeSet::<KeyDyn>::all();
    let mut rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges)
        .map(|row| {
            let mut cells = row.0.into_iter();
            let id_cell = cells.next().expect("id cell");
            let value_cell = cells.next().expect("value cell");

            let id = match id_cell {
                Some(DynCell::Str(value)) => value,
                _ => panic!("unexpected id cell"),
            };
            let value = match value_cell {
                Some(DynCell::I32(v)) => v,
                _ => panic!("unexpected value cell"),
            };
            (id, value)
        })
        .collect();
    rows.sort();
    assert_eq!(rows, vec![("user-1".into(), 10), ("user-2".into(), 20)]);

    drop(recovered);
    fs::remove_dir_all(&wal_dir)?;

    Ok(())
}
