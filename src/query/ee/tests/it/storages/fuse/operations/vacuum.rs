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

use std::sync::Arc;

use chrono::Duration;
use chrono::Utc;
use common_base::base::tokio;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_storages_fuse::FuseTable;
use databend_query::test_kits::table_test_fixture::append_sample_data;
use databend_query::test_kits::table_test_fixture::check_data_dir;
use databend_query::test_kits::table_test_fixture::execute_command;
use databend_query::test_kits::table_test_fixture::execute_query;
use databend_query::test_kits::table_test_fixture::TestFixture;
use databend_query::test_kits::utils::generate_orphan_files;
use databend_query::test_kits::utils::generate_snapshot_with_segments;
use databend_query::test_kits::utils::query_count;
use enterprise_query::storages::fuse::do_vacuum;
use enterprise_query::storages::fuse::do_vacuum_drop_tables;
use enterprise_query::storages::fuse::operations::vacuum_table::get_snapshot_referenced_files;
use enterprise_query::storages::fuse::operations::vacuum_table::SnapshotReferencedFiles;

async fn check_files_existence(
    fixture: &TestFixture,
    exist_files: Vec<&Vec<String>>,
    not_exist_files: Vec<&Vec<String>>,
) -> Result<()> {
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let dal = fuse_table.get_operator_ref();
    for file_vec in &not_exist_files {
        for file in file_vec.iter() {
            assert!(!dal.is_exist(file).await?);
        }
    }

    for file_vec in &exist_files {
        for file in file_vec.iter() {
            assert!(dal.is_exist(file).await?);
        }
    }

    Ok(())
}

async fn check_dry_run_files(fixture: &TestFixture, files: Vec<&Vec<String>>) -> Result<()> {
    let ctx = fixture.ctx();
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let retention_time = chrono::Utc::now() - chrono::Duration::seconds(2);
    let files_opt = do_vacuum(fuse_table, table_ctx, retention_time, true).await?;

    assert!(files_opt.is_some());
    let purge_files = files_opt.unwrap();
    for file_vec in &files {
        for file in file_vec.iter() {
            assert!(purge_files.contains(file));
        }
    }
    let mut count = 0;
    files.iter().for_each(|f| count += f.len());
    assert_eq!(purge_files.len(), count);
    Ok(())
}

// return all the snapshot files and referenced files.
async fn get_snapshots_and_referenced_files(
    fixture: &TestFixture,
) -> Result<(SnapshotReferencedFiles, Vec<String>)> {
    let ctx = fixture.ctx();
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let referenced_files = get_snapshot_referenced_files(fuse_table, &table_ctx)
        .await?
        .unwrap();
    let snapshot_files = fuse_table.list_snapshot_files().await?;

    Ok((referenced_files, snapshot_files))
}

// return generate orphan files and snapshot file(optional).
async fn generate_test_orphan_files(
    fixture: &TestFixture,
    genetate_snapshot: bool,
    orphan_segment_file_num: usize,
    orphan_block_per_segment_num: usize,
) -> Result<(Vec<String>, Option<String>)> {
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let files = generate_orphan_files(
        fuse_table,
        orphan_segment_file_num,
        orphan_block_per_segment_num,
    )
    .await?;

    let snapshot_opt = if genetate_snapshot {
        let mut segments = Vec::with_capacity(files.len());
        files.iter().for_each(|(location, _)| {
            segments.push(location.clone());
        });

        let new_timestamp = Utc::now() - Duration::minutes(1);
        let snapshot_location =
            generate_snapshot_with_segments(fuse_table, segments, Some(new_timestamp)).await?;

        Some(snapshot_location)
    } else {
        None
    };

    let mut orphan_files = vec![];
    for (location, segment) in files {
        orphan_files.push(location.0);
        for block_meta in segment.blocks {
            orphan_files.push(block_meta.location.0.clone());
            if let Some(block_index) = &block_meta.bloom_filter_index_location {
                orphan_files.push(block_index.0.clone());
            }
        }
    }

    Ok((orphan_files, snapshot_opt))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_drop_table() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    table_ctx.get_settings().set_retention_period(0)?;
    fixture.create_default_table().await?;

    let number_of_block = 1;
    append_sample_data(number_of_block, &fixture).await?;

    let table = fixture.latest_default_table().await?;

    check_data_dir(
        &fixture,
        "test_fuse_do_vacuum_drop_table: verify generate files",
        1,
        0,
        1,
        1,
        1,
        None,
        None,
    )
    .await?;

    // do gc.
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let qry = format!("drop table {}.{}", db, tbl);
    let ctx = fixture.ctx();
    execute_command(ctx, &qry).await?;

    // verify dry run never delete files
    {
        do_vacuum_drop_tables(vec![table.clone()], Some(100)).await?;
        check_data_dir(
            &fixture,
            "test_fuse_do_vacuum_drop_table: verify generate files",
            1,
            0,
            1,
            1,
            1,
            None,
            None,
        )
        .await?;
    }

    {
        do_vacuum_drop_tables(vec![table], None).await?;

        // after vacuum drop tables, verify the files number
        check_data_dir(
            &fixture,
            "test_fuse_do_vacuum_drop_table: verify generate retention files",
            0,
            0,
            0,
            0,
            0,
            None,
            None,
        )
        .await?;
    }
    Ok(())
}
