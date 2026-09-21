// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod common;

use std::sync::Arc;

use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use chrono::DateTime;
use chrono::Utc;
use common::TestWarehouse;
use common::databend_table;
use common::filesystem_catalog;
use databend_common_base::base::Progress;
use databend_common_catalog::table::Table;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;
use databend_common_storages_paimon::PaimonCommitMeta;
use databend_common_storages_paimon::PaimonCommitSink;
use databend_common_storages_paimon::PaimonTable;
use databend_common_storages_paimon::PaimonTableWriter;
use futures::StreamExt;
use paimon::Catalog;
use paimon::SnapshotManager;
use paimon::catalog::Identifier;
use paimon::spec::DataFileMeta;
use paimon::spec::DataType as PaimonDataType;
use paimon::spec::IndexFileMeta;
use paimon::spec::IntType;
use paimon::spec::Schema;
use paimon::spec::VarCharType;
use paimon::table::CommitMessage;

#[tokio::test]
async fn test_write_support_matrix() {
    let wh = TestWarehouse::new();
    let catalog = filesystem_catalog(&wh.warehouse);
    catalog
        .create_database("db", false, Default::default())
        .await
        .expect("create db");

    let append_id = create_matrix_table(&catalog, "append_t", MatrixKind::Append).await;
    let fixed_id = create_matrix_table(&catalog, "fixed_pk", MatrixKind::FixedPk).await;
    let dynamic_id = create_matrix_table(&catalog, "dynamic_pk", MatrixKind::DynamicPk).await;
    let postpone_id = create_matrix_table(&catalog, "postpone_pk", MatrixKind::PostponePk).await;
    let zero_id = create_matrix_table(&catalog, "zero_pk", MatrixKind::ZeroBucketPk).await;

    let append = databend_table(&wh.warehouse, &append_id).await;
    let fixed = databend_table(&wh.warehouse, &fixed_id).await;
    let dynamic = databend_table(&wh.warehouse, &dynamic_id).await;
    let postpone = databend_table(&wh.warehouse, &postpone_id).await;
    let zero = databend_table(&wh.warehouse, &zero_id).await;

    let append_table = as_paimon(append.as_ref());
    let fixed_pk_table = as_paimon(fixed.as_ref());
    let dynamic_pk_table = as_paimon(dynamic.as_ref());
    let postpone_pk_table = as_paimon(postpone.as_ref());
    let zero_pk_table = as_paimon(zero.as_ref());

    assert!(!append_table.is_read_only());
    assert!(append_table.support_distributed_insert());
    assert!(append_table.validate_insert(false).is_ok());
    assert!(fixed_pk_table.validate_insert(false).is_ok());
    assert!(
        dynamic_pk_table
            .validate_insert(false)
            .unwrap_err()
            .message()
            .contains("dynamic bucket")
    );
    assert!(
        postpone_pk_table
            .validate_insert(false)
            .unwrap_err()
            .message()
            .contains("postpone bucket")
    );
    assert!(
        fixed_pk_table
            .validate_insert(true)
            .unwrap_err()
            .message()
            .contains("overwrite")
    );

    let zero_err = zero_pk_table.validate_insert(false).unwrap_err().message();
    assert!(zero_err.contains("bucket < 1"), "{zero_err}");
    assert!(zero_err.contains("bucket=0"), "{zero_err}");
    assert!(zero_err.contains("write_mode=insert"), "{zero_err}");
    assert!(zero_err.contains("zero_pk"), "{zero_err}");

    let overwrite_err = fixed_pk_table.validate_insert(true).unwrap_err().message();
    assert!(
        overwrite_err.contains("write_mode=overwrite"),
        "{overwrite_err}"
    );
    assert!(overwrite_err.contains("fixed_pk"), "{overwrite_err}");
}

#[test]
fn test_commit_meta_round_trip() {
    let message = fixture_commit_message();
    let meta = PaimonCommitMeta::try_from_messages(vec![message.clone()]).unwrap();
    let json = serde_json::to_string(&meta).unwrap();
    let decoded: PaimonCommitMeta = serde_json::from_str(&json).unwrap();
    let restored = decoded.into_messages().unwrap();
    assert_eq!(restored[0].partition, message.partition);
    assert_eq!(restored[0].bucket, message.bucket);
    assert_eq!(restored[0].new_files, message.new_files);
    assert_eq!(restored[0].deleted_files, message.deleted_files);
    assert_eq!(restored[0].new_index_files, message.new_index_files);
}

#[tokio::test]
async fn test_parallel_writers_commit_one_snapshot() {
    let wh = TestWarehouse::new();
    let table = setup_empty_partitioned_pk_table(&wh.warehouse).await;
    let progress = Arc::new(Progress::create());

    let mut writer0 =
        PaimonTableWriter::try_create_with_progress(progress.clone(), table.clone()).unwrap();
    let mut writer1 =
        PaimonTableWriter::try_create_with_progress(progress.clone(), table.clone()).unwrap();

    // Different partitions => different (partition, bucket) lanes.
    writer0
        .transform(make_block(vec![0, 0, 0, 0], vec![1, 2, 3, 4], vec![
            "a", "b", "c", "d",
        ]))
        .await
        .unwrap();
    writer1
        .transform(make_block(vec![1, 1, 1, 1], vec![5, 6, 7, 8], vec![
            "e", "f", "g", "h",
        ]))
        .await
        .unwrap();

    let mut metas = Vec::new();
    if let Some(block) = writer0.on_finish(true).await.unwrap() {
        metas.push(block);
    }
    if let Some(block) = writer1.on_finish(true).await.unwrap() {
        metas.push(block);
    }
    assert_eq!(metas.len(), 2);

    let before = latest_snapshot_id(&table).await;
    commit_messages(&table, metas).await.unwrap();
    let after = latest_snapshot_id(&table).await;
    assert_eq!(after, before.map_or(Some(1), |id| Some(id + 1)));
    assert_eq!(read_all_rows(&table).await.unwrap().len(), 8);

    // Empty meta must not advance the snapshot.
    let before_empty = latest_snapshot_id(&table).await;
    commit_messages(&table, vec![]).await.unwrap();
    let after_empty = latest_snapshot_id(&table).await;
    assert_eq!(after_empty, before_empty);
}

#[tokio::test]
async fn test_empty_write_does_not_create_snapshot() {
    let wh = TestWarehouse::new();
    let table = setup_empty_partitioned_pk_table(&wh.warehouse).await;
    let progress = Arc::new(Progress::create());
    let mut writer = PaimonTableWriter::try_create_with_progress(progress, table.clone()).unwrap();

    assert!(
        writer
            .transform(DataBlock::empty())
            .await
            .unwrap()
            .is_none()
    );
    assert!(writer.on_finish(true).await.unwrap().is_none());

    let before = latest_snapshot_id(&table).await;
    commit_messages(&table, vec![]).await.unwrap();
    assert_eq!(latest_snapshot_id(&table).await, before);
    assert!(read_all_rows(&table).await.unwrap().is_empty());
}

#[tokio::test]
async fn test_unsunk_meta_does_not_commit() {
    // Writer prepare can succeed and emit meta; without delivering it to the
    // sink (pipeline abort / CALL_ON_FINISH_ON_ERROR=false), no snapshot is created.
    const {
        assert!(
            !PaimonCommitSink::CALL_ON_FINISH_ON_ERROR,
            "abort after partial consume must not call on_finish/commit"
        );
    }

    let wh = TestWarehouse::new();
    let table = setup_empty_partitioned_pk_table(&wh.warehouse).await;
    let progress = Arc::new(Progress::create());
    let mut writer = PaimonTableWriter::try_create_with_progress(progress, table.clone()).unwrap();

    writer
        .transform(make_block(vec![0], vec![1], vec!["x"]))
        .await
        .unwrap();
    let meta_block = writer.on_finish(true).await.unwrap().expect("meta");

    // Meta produced but never sunk — mirrors pipeline abort skipping on_finish.
    let before = latest_snapshot_id(&table).await;
    assert!(before.is_none());
    assert!(read_all_rows(&table).await.unwrap().is_empty());

    // Committing the same meta later still works (files already on disk).
    commit_messages(&table, vec![meta_block]).await.unwrap();
    assert_eq!(latest_snapshot_id(&table).await, Some(1));
    assert_eq!(read_all_rows(&table).await.unwrap().len(), 1);
}

fn fixture_commit_message() -> CommitMessage {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let wh = TestWarehouse::new();
        let catalog = filesystem_catalog(&wh.warehouse);
        catalog
            .create_database("db", false, Default::default())
            .await
            .expect("create db");

        let schema = Schema::builder()
            .column("id", PaimonDataType::Int(IntType::new()))
            .column("value", PaimonDataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "1")
            .build()
            .expect("schema");
        let identifier = Identifier::new("db", "commit_meta");
        catalog
            .create_table(&identifier, schema, false)
            .await
            .expect("create table");
        let table = catalog.get_table(&identifier).await.expect("get table");

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ])
        .expect("record batch");

        let write_builder = table.new_write_builder();
        let mut table_write = write_builder.new_write().expect("table write");
        table_write
            .write_arrow_batch(&batch)
            .await
            .expect("write batch");
        let mut messages = table_write.prepare_commit().await.expect("prepare commit");
        assert!(!messages.is_empty(), "expected at least one commit message");

        let mut message = messages.remove(0);
        // DataFileMeta serde stores creation_time as epoch millis; truncate so the
        // fixture matches what a JSON round-trip can preserve.
        truncate_creation_time_to_millis(&mut message.new_files);
        // Exercise deleted_files / new_index_files round-trip with realistic nested metas.
        message.deleted_files = message.new_files.clone();
        message.new_index_files = vec![IndexFileMeta {
            index_type: "HASH".to_string(),
            file_name: "index-0".to_string(),
            file_size: 32,
            row_count: 1,
            deletion_vectors_ranges: None,
            global_index_meta: None,
        }];
        message
    })
}

fn truncate_creation_time_to_millis(files: &mut [DataFileMeta]) {
    for file in files {
        if let Some(ts) = file.creation_time {
            file.creation_time = DateTime::<Utc>::from_timestamp_millis(ts.timestamp_millis());
        }
    }
}

async fn setup_empty_partitioned_pk_table(warehouse: &str) -> paimon::Table {
    let catalog = filesystem_catalog(warehouse);
    catalog
        .create_database("db", false, Default::default())
        .await
        .expect("create db");
    let schema = Schema::builder()
        .column("part", PaimonDataType::Int(IntType::new()))
        .column("id", PaimonDataType::Int(IntType::new()))
        .column("value", PaimonDataType::VarChar(VarCharType::string_type()))
        .partition_keys(["part"])
        .primary_key(["part", "id"])
        .option("bucket", "2")
        .build()
        .expect("schema");
    let identifier = Identifier::new("db", "parallel_write");
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("create table");
    catalog.get_table(&identifier).await.expect("get table")
}

fn make_block(parts: Vec<i32>, ids: Vec<i32>, values: Vec<&str>) -> DataBlock {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("part", ArrowDataType::Int32, false),
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(parts)),
        Arc::new(Int32Array::from(ids)),
        Arc::new(StringArray::from(values)),
    ])
    .expect("record batch");
    let schema = DataSchema::new(vec![
        DataField::new("part", DataType::Number(NumberDataType::Int32)),
        DataField::new("id", DataType::Number(NumberDataType::Int32)),
        DataField::new("value", DataType::String),
    ]);
    DataBlock::from_record_batch(&schema, &batch).expect("datablock")
}

async fn latest_snapshot_id(table: &paimon::Table) -> Option<i64> {
    let manager = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    manager
        .get_latest_snapshot_id()
        .await
        .expect("latest snapshot id")
}

async fn commit_messages(
    table: &paimon::Table,
    metas: Vec<DataBlock>,
) -> databend_common_exception::Result<()> {
    let mut sink = PaimonCommitSink::new(table.clone());
    for block in metas {
        sink.consume(block).await?;
    }
    sink.on_finish().await
}

async fn read_all_rows(
    table: &paimon::Table,
) -> databend_common_exception::Result<Vec<(i32, i32, String)>> {
    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.expect("scan plan");
    let table_read = read_builder.new_read().expect("table read");
    let mut rows = Vec::new();
    for split in plan.splits() {
        let mut stream = table_read
            .to_arrow(std::slice::from_ref(split))
            .expect("arrow stream");
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch");
            let parts = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("part");
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id");
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("value");
            for i in 0..batch.num_rows() {
                rows.push((parts.value(i), ids.value(i), values.value(i).to_string()));
            }
        }
    }
    Ok(rows)
}

enum MatrixKind {
    Append,
    FixedPk,
    DynamicPk,
    PostponePk,
    ZeroBucketPk,
}

async fn create_matrix_table(
    catalog: &paimon::FileSystemCatalog,
    name: &str,
    kind: MatrixKind,
) -> Identifier {
    let mut builder = Schema::builder()
        .column("id", PaimonDataType::Int(IntType::new()))
        .column("value", PaimonDataType::VarChar(VarCharType::string_type()));
    builder = match kind {
        MatrixKind::Append => builder,
        MatrixKind::FixedPk => builder.primary_key(["id"]).option("bucket", "1"),
        MatrixKind::DynamicPk => builder.primary_key(["id"]).option("bucket", "-1"),
        MatrixKind::PostponePk => builder.primary_key(["id"]).option("bucket", "-2"),
        MatrixKind::ZeroBucketPk => builder.primary_key(["id"]).option("bucket", "0"),
    };
    let schema = builder.build().expect("schema");
    let identifier = Identifier::new("db", name);
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("create table");
    identifier
}

fn as_paimon(table: &dyn Table) -> &PaimonTable {
    table
        .as_any()
        .downcast_ref::<PaimonTable>()
        .expect("paimon table")
}
