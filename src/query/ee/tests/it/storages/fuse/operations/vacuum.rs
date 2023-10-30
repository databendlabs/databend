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
use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common_base::base::tokio;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_storages_fuse::FuseTable;
use databend_query::test_kits::table_test_fixture::append_sample_data;
use databend_query::test_kits::table_test_fixture::check_data_dir;
use databend_query::test_kits::table_test_fixture::execute_command;
use databend_query::test_kits::table_test_fixture::execute_query;
use databend_query::test_kits::table_test_fixture::get_data_dir_files;
use databend_query::test_kits::table_test_fixture::TestFixture;
use databend_query::test_kits::utils::generate_orphan_files;
use databend_query::test_kits::utils::generate_snapshot_with_segments;
use enterprise_query::storages::fuse::do_vacuum;
use enterprise_query::storages::fuse::do_vacuum_drop_tables;
use futures::TryStreamExt;

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

    let mut orphan_files = vec![];
    let snapshot_opt = if genetate_snapshot {
        let mut segments = Vec::with_capacity(files.len());
        files.iter().for_each(|(location, _)| {
            segments.push(location.clone());
        });

        let new_timestamp = Utc::now() - Duration::minutes(1);
        let snapshot_location =
            generate_snapshot_with_segments(fuse_table, segments, Some(new_timestamp)).await?;

        orphan_files.push(snapshot_location.clone());
        Some(snapshot_location)
    } else {
        None
    };

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

async fn check_dry_run_files(
    fixture: &TestFixture,
    files: HashSet<String>,
    retention_time: DateTime<Utc>,
) -> Result<()> {
    let ctx = fixture.ctx();
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let files_opt = do_vacuum(fuse_table, table_ctx, retention_time, true).await?;

    assert!(files_opt.is_some());
    let purge_files = files_opt.unwrap();
    assert_eq!(purge_files.len(), files.len());
    purge_files
        .into_iter()
        .for_each(|file| assert!(files.contains(&file)));

    Ok(())
}

async fn check_query_data(
    fixture: &TestFixture,
    query: &str,
    expected_data_blocks: &[DataBlock],
) -> Result<()> {
    let ctx = fixture.ctx();
    let data_blocks: Vec<DataBlock> = execute_query(ctx.clone(), query)
        .await?
        .try_collect()
        .await?;
    for (i, data_block) in data_blocks.iter().enumerate() {
        let expected_data_block = &expected_data_blocks[i];
        let expetecd_block_entries = expected_data_block.columns();
        let block_entries = data_block.columns();
        assert_eq!(expetecd_block_entries.len(), block_entries.len());

        for (j, entry) in block_entries.iter().enumerate() {
            let expected_entry = &expetecd_block_entries[j];
            assert_eq!(entry.data_type, expected_entry.data_type);
            assert_eq!(entry.value, expected_entry.value);
        }
    }

    Ok(())
}

async fn check_vacuum(
    fixture: &TestFixture,
    files: HashSet<String>,
    case_name: &str,
    retention_time: DateTime<Utc>,
    expected_num_of_snapshot: u32,
    expected_num_of_segment: u32,
    expected_num_of_blocks: u32,
) -> Result<()> {
    // do dry_run vacuum
    check_dry_run_files(fixture, files, retention_time).await?;

    // do vacuum
    let ctx = fixture.ctx();
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    do_vacuum(fuse_table, table_ctx, retention_time, false).await?;

    // after generate purgable data, verify the files number
    check_data_dir(
        fixture,
        case_name,
        expected_num_of_snapshot,
        0,
        expected_num_of_segment,
        expected_num_of_blocks,
        expected_num_of_blocks,
        None,
        None,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vacuum_orphan_files() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    fixture.create_default_table().await?;

    let data_qry = format!(
        "select * from {}.{} order by id",
        fixture.default_db_name(),
        fixture.default_table_name()
    );

    // first append some data and save snapshot
    let number_of_block = 1;
    append_sample_data(number_of_block, &fixture).await?;
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot_loc = fuse_table.snapshot_loc().await?.unwrap();

    // generate some orphan files
    let orphan_segment_file_num = 1;
    let orphan_block_per_segment_num = 1;

    // generate orphan snapshot and segments
    let (mut orphan_files, _) = generate_test_orphan_files(
        &fixture,
        true,
        orphan_segment_file_num,
        orphan_block_per_segment_num,
    )
    .await?;
    // save first snapshot location into orphan_files
    orphan_files.push(snapshot_loc);
    // generate orphan segments(without snapshot)
    let (orphan_files_2, _) = generate_test_orphan_files(
        &fixture,
        false,
        orphan_segment_file_num,
        orphan_block_per_segment_num,
    )
    .await?;
    orphan_files.extend(orphan_files_2);

    table_ctx.get_settings().set_retention_period(0)?;
    let retention_time = chrono::Utc::now();
    check_vacuum(
        &fixture,
        HashSet::new(),
        "test_fuse_vacuum_orphan_files step 1: verify files",
        retention_time,
        2,
        3,
        3,
    )
    .await?;

    // append some data and do vacuum
    // it will not vacuum any files, cause orphan file has the same table version with last snapshot
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    append_sample_data(number_of_block, &fixture).await?;
    let retention_time = chrono::Utc::now() - chrono::Duration::seconds(3);
    let orig_data_blocks: Vec<DataBlock> = execute_query(ctx.clone(), data_qry.as_str())
        .await?
        .try_collect()
        .await?;
    check_vacuum(
        &fixture,
        HashSet::new(),
        "test_fuse_vacuum_orphan_files step 2: verify files",
        retention_time,
        3,
        4,
        4,
    )
    .await?;
    check_query_data(&fixture, &data_qry, &orig_data_blocks).await?;

    // save the last snapshot into orphan_files
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    orphan_files.push(fuse_table.snapshot_loc().await?.unwrap());

    // sleep and append some data again
    // this time will vacuum all the orphan files and first snapshot file
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    append_sample_data(number_of_block, &fixture).await?;
    let retention_time = chrono::Utc::now() - chrono::Duration::seconds(3);
    let orig_data_blocks: Vec<DataBlock> = execute_query(ctx.clone(), data_qry.as_str())
        .await?
        .try_collect()
        .await?;
    // check that orphan files and first snapshot file has been purged
    let purge_files: HashSet<String> = orphan_files.into_iter().collect();
    check_vacuum(
        &fixture,
        purge_files,
        "test_fuse_vacuum_orphan_files step 2: verify files",
        retention_time,
        1,
        3,
        3,
    )
    .await?;
    check_query_data(&fixture, &data_qry, &orig_data_blocks).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vacuum_truncate_files() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    fixture.create_default_table().await?;

    let data_qry = format!(
        "select * from {}.{} order by id",
        fixture.default_db_name(),
        fixture.default_table_name()
    );

    // make some purgable data
    let (purgeable_files, _truncated_snapshot_loc) = {
        let number_of_block = 1;
        append_sample_data(number_of_block, &fixture).await?;

        let table = fixture.latest_default_table().await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot_loc = fuse_table.snapshot_loc().await?.unwrap();

        let data_blocks: Vec<DataBlock> = execute_query(ctx.clone(), data_qry.as_str())
            .await?
            .try_collect()
            .await?;
        assert!(!data_blocks.is_empty());

        let table = fixture.latest_default_table().await?;
        let _ = table.truncate(ctx.clone()).await;

        // after truncate check data is empty
        let data_blocks: Vec<DataBlock> = execute_query(ctx.clone(), data_qry.as_str())
            .await?
            .try_collect()
            .await?;
        assert!(data_blocks.is_empty());
        let files = get_data_dir_files(None).await?;

        (files, snapshot_loc)
    };

    table_ctx.get_settings().set_retention_period(0)?;

    // step 1. sleep 2s, and vacuum with now - 2s, it will not delete any files cause cannot file root gc snapshot
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let retention_time = chrono::Utc::now() - chrono::Duration::seconds(2);
    check_vacuum(
        &fixture,
        HashSet::new(),
        "test_fuse_vacuum_truncate_files step 1: verify files",
        retention_time,
        2,
        1,
        1,
    )
    .await?;

    // step 2: append some data, it will vacuum all the truncated files
    let number_of_block = 1;
    append_sample_data(number_of_block, &fixture).await?;
    let orig_data_blocks: Vec<DataBlock> = execute_query(ctx.clone(), data_qry.as_str())
        .await?
        .try_collect()
        .await?;
    assert!(!orig_data_blocks.is_empty());
    let retention_time = chrono::Utc::now() - chrono::Duration::seconds(2);
    check_vacuum(
        &fixture,
        purgeable_files,
        "test_fuse_vacuum_truncate_files step 2: verify files",
        retention_time,
        1,
        1,
        1,
    )
    .await?;
    check_query_data(&fixture, &data_qry, &orig_data_blocks).await?;

    Ok(())
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
