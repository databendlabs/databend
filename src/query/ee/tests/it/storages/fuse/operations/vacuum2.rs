// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;
use databend_storages_common_io::dedup_file_locations;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use futures::TryStreamExt;

// TODO investigate this
// NOTE: SHOULD specify flavor = "multi_thread", otherwise query execution might be hanged
#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum2_all() -> anyhow::Result<()> {
    let ee_setup = EESetup::new();
    let fixture = TestFixture::setup_with_custom(ee_setup).await?;
    // Adjust retention period to 0, so that dropped tables will be vacuumed immediately
    let session = fixture.default_session();
    session.get_settings().set_data_retention_time_in_days(0)?;

    let ctx = fixture.new_query_ctx().await?;

    let setup_statements = vec![
        // create non-system db1, create fuse and non-fuse table in it.
        "create database db1",
        "create table db1.t1 (c int) as select 1",
        "insert into db1.t1 values (1)",
        "truncate table db1.t1",
        "create table db1.t2 (c int) engine = memory as select 1",
        "truncate table db1.t2",
        // create fuse and non-fuse tables in default db
        "create table default.t1 (c int) as select 1",
        "insert into default.t1 values (1)",
        "truncate table default.t1",
        "create table default.t2 (c int) engine = memory as select 1",
        "truncate table default.t2",
    ];

    for stmt in setup_statements {
        fixture.execute_command(stmt).await?;
    }

    // vacuum them all
    let res = fixture.execute_command("call system$fuse_vacuum2()").await;

    // Check that:

    // 1. non-fuse tables should not stop us

    assert!(res.is_ok());

    //  2. fuse table data should be vacuumed

    let storage_root = fixture.storage_root();

    async fn check_files_left(
        ctx: &QueryContext,
        storage_root: &str,
        db_name: &str,
        tbl_name: &str,
    ) -> Result<()> {
        let tenant = ctx.get_tenant();
        let table = ctx
            .get_default_catalog()?
            .get_table(&tenant, db_name, tbl_name)
            .await?;

        let db = ctx
            .get_default_catalog()?
            .get_database(&tenant, db_name)
            .await?;

        let path = Path::new(storage_root)
            .join(db.get_db_info().database_id.db_id.to_string())
            .join(table.get_id().to_string());

        let walker = walkdir::WalkDir::new(path).into_iter();

        let mut files_left = Vec::new();
        for entry in walker {
            let entry = entry.unwrap();
            if entry.file_type().is_file() {
                files_left.push(entry);
            }
        }

        // There should be one snapshot file and one snapshot hint file left
        assert_eq!(files_left.len(), 2);

        files_left.sort_by(|a, b| a.file_name().cmp(b.file_name()));
        // First is the only snapshot left
        files_left[0].path().to_string_lossy().contains("/_ss/");
        // Second one is the last snapshot location hint
        files_left[1]
            .path()
            .to_string_lossy()
            .contains("last_snapshot_location_hint_v2");
        Ok::<(), ErrorCode>(())
    }

    check_files_left(&ctx, storage_root, "db1", "t1").await?;
    check_files_left(&ctx, storage_root, "default", "t1").await?;

    Ok(())
}

/// Regression test for chunked reads of protected gc-root segments.
///
/// `do_vacuum2` reads the gc-root's protected segments in chunks of
/// `VACUUM2_SEGMENT_READ_CHUNK_SIZE` (1000) rather than all at once, so that peak
/// memory does not scale with the number of protected segments. Every chunk must
/// contribute its block locations to the protected set: if a single chunk were
/// skipped or its results dropped, the blocks it protects would be misclassified
/// as garbage and deleted, silently corrupting a live table.
///
/// This test forces the segment count past the chunk boundary (one block per
/// segment, one row per block) and then asserts every block still referenced by
/// the surviving snapshot is present on storage after vacuum.
#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum2_protected_segments_span_multiple_chunks() -> anyhow::Result<()> {
    // Must exceed the chunk size used by do_vacuum2, so the read loop runs more
    // than one iteration and a lost chunk would be observable.
    const SEGMENT_COUNT: usize = 1001;

    let ee_setup = EESetup::new();
    let fixture = TestFixture::setup_with_custom(ee_setup).await?;

    let session = fixture.default_session();
    // Retention 0 so the previous snapshot becomes collectable immediately.
    session.get_settings().set_data_retention_time_in_days(0)?;
    // Auto compaction would merge the many tiny segments back together and
    // defeat the point of the test.
    session
        .get_settings()
        .set_auto_compaction_imperfect_blocks_threshold(0)?;

    let ctx = fixture.new_query_ctx().await?;
    fixture.create_default_database().await?;
    let db_name = fixture.default_db_name();
    let tbl_name = "t_chunked";

    fixture
        .execute_command(&format!(
            "create table {}.{} (c int) row_per_block=1 block_per_segment=1",
            db_name, tbl_name
        ))
        .await?;

    // One row per block and one block per segment, so each row lands in its own
    // segment and the gc-root ends up with SEGMENT_COUNT protected segments.
    fixture
        .execute_command(&format!(
            "insert into {}.{} select number from numbers({})",
            db_name, tbl_name, SEGMENT_COUNT
        ))
        .await?;

    // A second write creates a newer snapshot, so the first one becomes eligible
    // for collection and vacuum has actual work to do.
    fixture
        .execute_command(&format!("insert into {}.{} values (-1)", db_name, tbl_name))
        .await?;

    // Confirm the setup actually crossed the chunk boundary. If table option
    // semantics change and segments get merged, the test would silently stop
    // covering the multi-chunk path, so fail loudly instead.
    let table = ctx
        .get_default_catalog()?
        .get_table(&ctx.get_tenant(), &db_name, tbl_name)
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = fuse_table
        .read_table_snapshot()
        .await?
        .expect("snapshot should exist after inserts");
    assert!(
        snapshot.segments.len() > 1000,
        "expected more than 1000 protected segments to cross the chunk boundary, got {}",
        snapshot.segments.len()
    );

    // Blocks referenced by the surviving snapshot: none of these may be removed.
    let segments_io =
        SegmentsIO::create(ctx.clone(), fuse_table.get_operator(), fuse_table.schema());
    let mut live_blocks = Vec::new();
    for chunk in snapshot.segments.chunks(500) {
        let segments = segments_io
            .read_segments::<Arc<CompactSegmentInfo>>(chunk, false)
            .await?;
        for segment in segments {
            for block in segment?.block_metas()?.iter() {
                live_blocks.push(block.location.0.clone());
            }
        }
    }
    assert_eq!(live_blocks.len(), SEGMENT_COUNT + 1);

    fixture
        .execute_command(&format!(
            "call system$fuse_vacuum2('{}', '{}')",
            db_name, tbl_name
        ))
        .await?;

    // The core assertion: every block still referenced by the live snapshot must
    // survive. Dropping any protected-segment chunk during the read would leave
    // that chunk's blocks unprotected and delete them here.
    let operator = fuse_table.get_operator();
    let mut missing = Vec::new();
    for loc in &live_blocks {
        if !operator.exists(loc).await? {
            missing.push(loc.clone());
        }
    }
    assert!(
        missing.is_empty(),
        "vacuum deleted {} live block(s) still referenced by the current snapshot; \
         first few: {:?}",
        missing.len(),
        &missing[..missing.len().min(5)]
    );

    // Sanity check that the table is still readable end to end and no rows were
    // lost: scanning every row would fail outright if a live block was deleted.
    let stream = fixture
        .execute_query(&format!("select c from {}.{}", db_name, tbl_name))
        .await?;
    let blocks: Vec<DataBlock> = stream.try_collect().await?;
    let scanned_rows: usize = blocks.iter().map(|b| b.num_rows()).sum();
    assert_eq!(scanned_rows, SEGMENT_COUNT + 1);

    Ok(())
}

/// An old transaction may have written block objects before a newer snapshot becomes the
/// vacuum gc-root. Vacuum may delete those uncommitted objects, but the old transaction must
/// then fail to commit so no committed snapshot can reference a deleted block.
#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum2_rejects_transaction_whose_blocks_were_collected() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;

    let db_name = fixture.default_db_name();
    let tbl_name = "t_concurrent_txn";
    fixture
        .execute_command(&format!("create table {db_name}.{tbl_name} (c int)"))
        .await?;
    fixture
        .execute_command(&format!("insert into {db_name}.{tbl_name} values (1)"))
        .await?;

    let catalog_ctx = fixture.new_query_ctx().await?;
    let table = catalog_ctx
        .get_default_catalog()?
        .get_table(&fixture.default_tenant(), &db_name, tbl_name)
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse_table.get_operator();
    let block_prefix = fuse_table
        .meta_location_generator()
        .block_location_prefix()
        .to_string();
    let blocks_before_txn = operator
        .list(&block_prefix)
        .await?
        .into_iter()
        .filter(|entry| entry.metadata().is_file())
        .map(|entry| entry.path().to_string())
        .collect::<HashSet<_>>();

    let txn_session = fixture.new_session_with_type(SessionType::Dummy).await?;
    txn_session
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    let txn_ctx = txn_session
        .create_query_context(&databend_common_version::BUILD_INFO)
        .await?;
    execute_command(txn_ctx.clone(), "begin").await?;
    execute_command(
        txn_ctx.clone(),
        &format!("insert into {db_name}.{tbl_name} values (2)"),
    )
    .await?;

    let blocks_after_txn = operator
        .list(&block_prefix)
        .await?
        .into_iter()
        .filter(|entry| entry.metadata().is_file())
        .map(|entry| entry.path().to_string())
        .collect::<HashSet<_>>();
    let txn_blocks = blocks_after_txn
        .difference(&blocks_before_txn)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        !txn_blocks.is_empty(),
        "transaction insert should write at least one uncommitted block"
    );

    // Ensure the next committed snapshot has a strictly later logical timestamp than the
    // transaction's block objects and can therefore become their gc-root cutoff.
    tokio::time::sleep(Duration::from_millis(2)).await;
    fixture
        .execute_command(&format!("insert into {db_name}.{tbl_name} values (3)"))
        .await?;
    fixture
        .execute_command(&format!(
            "call system$fuse_vacuum2('{db_name}', '{tbl_name}')"
        ))
        .await?;

    for path in &txn_blocks {
        assert!(
            !operator.exists(path).await?,
            "vacuum should collect uncommitted block older than the gc-root: {path}"
        );
    }

    let commit_error = execute_command(txn_ctx, "commit")
        .await
        .expect_err("transaction must not commit a snapshot referencing collected blocks");
    assert!(
        matches!(
            commit_error.code(),
            ErrorCode::STORAGE_NOT_FOUND
                | ErrorCode::TABLE_VERSION_MISMATCHED
                | ErrorCode::UNRESOLVABLE_CONFLICT
        ),
        "unexpected transaction rejection after vacuum collected its blocks: {commit_error}"
    );

    let stream = fixture
        .execute_query(&format!("select c from {db_name}.{tbl_name}"))
        .await?;
    let blocks: Vec<DataBlock> = stream.try_collect().await?;
    let committed_rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(committed_rows, 2);

    Ok(())
}

/// Verifies that dedup_file_locations correctly removes duplicates and reports samples.
#[test]
fn test_dedup_file_locations() {
    // Simulate the vacuum2 scenario: bloom index paths generated from blocks
    // with and without the 'h' prefix map to the same location.
    let mut locations = vec![
        "548052/604310/_i_b_v2/019bdabd0292702a96f51f3b3ea64335_v4.parquet".to_string(),
        "548052/604310/_i_b_v2/019bdabd02927061af2ed18c2562b79e_v4.parquet".to_string(),
        "548052/604310/_i_b_v2/019c0df29e8a7016a6a1be466e094ec3_v4.parquet".to_string(),
        // Duplicates (same paths appearing again)
        "548052/604310/_i_b_v2/019bdabd0292702a96f51f3b3ea64335_v4.parquet".to_string(),
        "548052/604310/_i_b_v2/019bdabd02927061af2ed18c2562b79e_v4.parquet".to_string(),
    ];

    let (duplicates, samples) = dedup_file_locations(&mut locations);

    assert_eq!(duplicates, 2);
    assert_eq!(locations.len(), 3);
    assert_eq!(samples.len(), 2);
    assert_eq!(
        samples[0],
        "548052/604310/_i_b_v2/019bdabd0292702a96f51f3b3ea64335_v4.parquet"
    );
    assert_eq!(
        samples[1],
        "548052/604310/_i_b_v2/019bdabd02927061af2ed18c2562b79e_v4.parquet"
    );
}

#[test]
fn test_dedup_file_locations_no_duplicates() {
    let mut locations = vec![
        "a/b/file1.parquet".to_string(),
        "a/b/file2.parquet".to_string(),
        "a/b/file3.parquet".to_string(),
    ];

    let (duplicates, samples) = dedup_file_locations(&mut locations);

    assert_eq!(duplicates, 0);
    assert_eq!(locations.len(), 3);
    assert!(samples.is_empty());
}
