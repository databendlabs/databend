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
use databend_query::test_kits::table_test_fixture::execute_query;
use databend_query::test_kits::table_test_fixture::TestFixture;
use databend_query::test_kits::utils::generate_orphan_files;
use databend_query::test_kits::utils::generate_snapshot_with_segments;
use databend_query::test_kits::utils::query_count;
use enterprise_query::storages::fuse::do_vacuum;
use enterprise_query::storages::fuse::operations::vacuum::get_snapshot_referenced_files;
use enterprise_query::storages::fuse::operations::vacuum::SnapshotReferencedFiles;

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
    let files_opt = do_vacuum(fuse_table, table_ctx, retention_time, Some(1000)).await?;

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
async fn test_fuse_do_vacuum() -> Result<()> {
    // verifies that:
    //
    // - purge orphan {segments|blocks|blocks index} files, but that within retention period shall not be collected
    //
    // for example :
    //
    //   ──┬──           S_old
    //     │
    //   ──┬──           S_orphan
    //     │
    //   ──┬──           S_current
    //     │
    //  within retention orphan files
    //     │
    //     │             within_retention_time_orphan_files
    //   ──┴──
    //  outof retention orphan files
    //     │             outof_retention_time_orphan_files
    //
    // - S_old is the old gc root
    //   all the {segments|blocks|blocks index} referenced by S_old MUST NOT be collected
    //
    // - S_orphan is the orphan snapshot
    //   all the {segments|blocks|blocks index} referenced by S_orphan MUST NOT be collected
    //
    // - S_current is the gc root
    //   all the {segments|blocks|blocks index} referenced by S_current MUST NOT be collected
    //
    // - within_retention_time_orphan_files shall NOT be purged
    //   since they are within retention time.
    // - outof_retention_time_orphan_files should be purged
    //   since it is outof the retention period
    //
    //  put them together, after GC, there will be
    //  - 2 snapshots left: s_current, s_old
    //  - 3 segments left: two referenced by S_current and s_old, one in within_retention_time_orphan_files
    //  - 2 blocks left: two referenced by S_current and old, one in within_retention_time_orphan_files
    //
    //  this test case test that:
    //  - all the files referenced by snapshot(no matter if or not is an orphan snapshot),
    //    if or not within retention time, will not be purged;
    //  - all the orphan files that not referenced by any snapshots:
    //      - if within retention, will not be purged
    //      - if outof retention, will be purged

    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    table_ctx.get_settings().set_retention_period(0)?;
    fixture.create_default_table().await?;

    let count_qry = format!(
        "select count(*) from {}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    assert_eq!(0, {
        let stream = execute_query(fixture.ctx(), &count_qry).await?;
        query_count(stream).await? as usize
    });

    let orphan_segment_file_num = 1;
    let orphan_block_per_segment_num = 1;

    // generate some orphan files out of retention time
    let (outof_retention_time_orphan_files, _) = generate_test_orphan_files(
        &fixture,
        false,
        orphan_segment_file_num,
        orphan_block_per_segment_num,
    )
    .await?;

    let number_of_block = 1;
    append_sample_data(number_of_block, &fixture).await?;

    let first_append_data_count = {
        let stream = execute_query(fixture.ctx(), &count_qry).await?;
        query_count(stream).await? as usize
    };
    assert!(first_append_data_count > 0);

    // generate orphan snapshot and segments
    let (mut orphan_snapshot_orphan_files, exist_orphan_snapshot_file) =
        generate_test_orphan_files(
            &fixture,
            true,
            orphan_segment_file_num,
            orphan_block_per_segment_num,
        )
        .await?;
    orphan_snapshot_orphan_files.push(exist_orphan_snapshot_file.clone().unwrap());

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // generate some orphan files within retention time
    let (within_retention_time_orphan_files, _) = generate_test_orphan_files(
        &fixture,
        false,
        orphan_segment_file_num,
        orphan_block_per_segment_num,
    )
    .await?;

    // after generate orphan files and insert data, verify the files number
    {
        let expected_num_of_snapshot = 2;
        let expected_num_of_segment = 1 + (orphan_segment_file_num * 3) as u32;
        let expected_num_of_blocks =
            1 + (orphan_segment_file_num * orphan_block_per_segment_num * 3) as u32;
        let expected_num_of_index = expected_num_of_blocks;
        check_data_dir(
            &fixture,
            "do_gc_orphan_files: verify generate retention files",
            expected_num_of_snapshot,
            0,
            expected_num_of_segment,
            expected_num_of_blocks,
            expected_num_of_index,
            None,
            None,
        )
        .await?;
    }

    // insert data to make some new snapshot files.
    append_sample_data(number_of_block, &fixture).await?;

    // after generate orphan files\insert data\analyze table, verify the files number
    {
        let expected_num_of_snapshot = 3;
        let expected_num_of_segment = 2 + (orphan_segment_file_num * 3) as u32;
        let expected_num_of_blocks =
            2 + (orphan_segment_file_num * orphan_block_per_segment_num * 3) as u32;
        let expected_num_of_index = expected_num_of_blocks;
        let expected_num_of_ts = 0;
        check_data_dir(
            &fixture,
            "do_gc_orphan_files: verify generate retention files and referenced files",
            expected_num_of_snapshot,
            expected_num_of_ts,
            expected_num_of_segment,
            expected_num_of_blocks,
            expected_num_of_index,
            Some(()),
            None,
        )
        .await?;
    }

    let data_count = {
        let stream = execute_query(fixture.ctx(), &count_qry).await?;
        query_count(stream).await? as usize
    };
    assert_eq!(data_count, first_append_data_count * 2);

    // do dry_run gc
    let purged_files = vec![
        &outof_retention_time_orphan_files,
        &orphan_snapshot_orphan_files,
    ];
    check_dry_run_files(&fixture, purged_files.clone()).await?;

    // check that after dry_run gc, files number has not changed
    {
        let expected_num_of_snapshot = 3;
        let expected_num_of_segment = 2 + (orphan_segment_file_num * 3) as u32;
        let expected_num_of_blocks =
            2 + (orphan_segment_file_num * orphan_block_per_segment_num * 3) as u32;
        let expected_num_of_index = expected_num_of_blocks;
        let expected_num_of_ts = 0;
        check_data_dir(
            &fixture,
            "do_gc_orphan_files: verify generate retention files and referenced files",
            expected_num_of_snapshot,
            expected_num_of_ts,
            expected_num_of_segment,
            expected_num_of_blocks,
            expected_num_of_index,
            Some(()),
            None,
        )
        .await?;
    }

    // do gc.
    {
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let table = fixture.latest_default_table().await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let retention_time = chrono::Utc::now() - chrono::Duration::seconds(2);
        do_vacuum(fuse_table, table_ctx, retention_time, None).await?;
    }

    assert_eq!(data_count, {
        let stream = execute_query(fixture.ctx(), &count_qry).await?;
        query_count(stream).await? as usize
    });

    // check files number
    {
        let expected_num_of_snapshot = 2;
        let expected_num_of_segment = 2 + orphan_segment_file_num as u32;
        let expected_num_of_blocks =
            2 + (orphan_segment_file_num * orphan_block_per_segment_num) as u32;
        let expected_num_of_index = expected_num_of_blocks;
        let expected_num_of_ts = 0;
        check_data_dir(
            &fixture,
            "do_gc_orphan_files: after gc",
            expected_num_of_snapshot,
            expected_num_of_ts,
            expected_num_of_segment,
            expected_num_of_blocks,
            expected_num_of_index,
            Some(()),
            None,
        )
        .await?;
    }

    check_files_existence(
        &fixture,
        vec![&within_retention_time_orphan_files],
        purged_files.clone(),
    )
    .await?;

    // save all the referenced files
    let (old_referenced_files, old_snapshot_files) =
        get_snapshots_and_referenced_files(&fixture).await?;

    // sleep more time to make `within_retention_time_orphan_files` out of retention time
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // after sleep, check that if `within_retention_time_orphan_files` has been purged.

    // do dry_run gc
    check_dry_run_files(&fixture, vec![&within_retention_time_orphan_files]).await?;

    // check that after dry_run gc, files number has not changed
    {
        let expected_num_of_snapshot = 2;
        let expected_num_of_segment = 2 + orphan_segment_file_num as u32;
        let expected_num_of_blocks =
            2 + (orphan_segment_file_num * orphan_block_per_segment_num) as u32;
        let expected_num_of_index = expected_num_of_blocks;
        let expected_num_of_ts = 0;
        check_data_dir(
            &fixture,
            "do_gc_orphan_files: after gc",
            expected_num_of_snapshot,
            expected_num_of_ts,
            expected_num_of_segment,
            expected_num_of_blocks,
            expected_num_of_index,
            Some(()),
            None,
        )
        .await?;
    }

    // do gc.
    {
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let table = fixture.latest_default_table().await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let retention_time = chrono::Utc::now() - chrono::Duration::seconds(2);
        do_vacuum(fuse_table, table_ctx, retention_time, None).await?;
    }

    // check files number
    {
        let expected_num_of_snapshot = 2;
        let expected_num_of_segment = 2 + (orphan_segment_file_num - 1) as u32;
        let expected_num_of_blocks =
            2 + ((orphan_segment_file_num - 1) * orphan_block_per_segment_num) as u32;
        let expected_num_of_index = expected_num_of_blocks;
        let expected_num_of_ts = 0;
        check_data_dir(
            &fixture,
            "do_gc_orphan_files: after gc",
            expected_num_of_snapshot,
            expected_num_of_ts,
            expected_num_of_segment,
            expected_num_of_blocks,
            expected_num_of_index,
            Some(()),
            None,
        )
        .await?;
    }

    check_files_existence(&fixture, vec![], vec![&within_retention_time_orphan_files]).await?;

    // check all referenced files and snapshot files exist
    {
        let (referenced_files, snapshot_files) =
            get_snapshots_and_referenced_files(&fixture).await?;

        assert_eq!(referenced_files, old_referenced_files);
        assert_eq!(old_snapshot_files, snapshot_files);

        check_files_existence(
            &fixture,
            vec![&referenced_files.all_files(), &snapshot_files],
            vec![&within_retention_time_orphan_files],
        )
        .await?;
    }

    // check data count matched.
    assert_eq!(data_count, {
        let stream = execute_query(fixture.ctx(), &count_qry).await?;
        query_count(stream).await? as usize
    });

    Ok(())
}
