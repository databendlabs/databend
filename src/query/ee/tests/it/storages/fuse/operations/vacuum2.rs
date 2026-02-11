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

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaReaders;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::TableSnapshot;

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

pub(crate) async fn snapshot_segment_and_block_locations(
    fuse_table: &FuseTable,
    snapshot: &TableSnapshot,
) -> anyhow::Result<(HashSet<String>, HashSet<String>)> {
    let reader = MetaReaders::segment_info_reader(fuse_table.get_operator(), fuse_table.schema());
    let mut segment_locations = HashSet::new();
    let mut block_locations = HashSet::new();

    for (location, ver) in &snapshot.segments {
        segment_locations.insert(location.clone());
        let segment = reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;

        for block_meta in segment.block_metas()? {
            block_locations.insert(block_meta.location.0.clone());
        }
    }

    Ok((segment_locations, block_locations))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum2_keeps_branch_refs() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command("set enable_experimental_table_ref=1")
        .await?;

    append_sample_data(1, &fixture).await?;

    let db = fixture.default_db_name();
    let table = fixture.default_table_name();
    let table_obj = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table_obj.as_ref())?;
    let snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    let (branch_segments, branch_blocks) =
        snapshot_segment_and_block_locations(fuse_table, snapshot.as_ref()).await?;
    assert!(!branch_segments.is_empty());
    assert!(!branch_blocks.is_empty());

    fixture
        .execute_command(format!("alter table {db}.{table} create branch old_branch").as_str())
        .await?;
    fixture
        .execute_command(format!("truncate table {db}.{table}").as_str())
        .await?;
    append_sample_data(1, &fixture).await?;

    let latest_table = fixture.latest_default_table().await?;
    let latest_fuse_table = FuseTable::try_from_table(latest_table.as_ref())?;
    let ctx: std::sync::Arc<dyn TableContext> = fixture.new_query_ctx().await?;
    let (ref_info, _) = latest_fuse_table
        .process_snapshot_refs_for_vacuum(&ctx, true, None)
        .await?;

    for location in &branch_segments {
        assert!(
            ref_info
                .protected_segments
                .iter()
                .any(|(path, _)| path == location),
            "segment referenced by branch was not protected for vacuum2: {location}"
        );
    }

    let protected_snapshot = TableSnapshot {
        segments: ref_info.protected_segments.iter().cloned().collect(),
        ..snapshot.as_ref().clone()
    };
    let (_, protected_blocks_from_segments) =
        snapshot_segment_and_block_locations(latest_fuse_table, &protected_snapshot).await?;
    for location in &branch_blocks {
        assert!(
            protected_blocks_from_segments.contains(location),
            "block referenced by branch was not indirectly protected by vacuum2 segments: {location}"
        );
    }

    Ok(())
}
