//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_query::test_kits::*;

use crate::storages::fuse::utils::do_insertions;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_snapshot_analyze() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;
    let case_name = "analyze_statistic_optimize";
    do_insertions(&fixture).await?;

    analyze_table(&fixture).await?;
    check_data_dir(&fixture, case_name, 3, 1, 2, 2, 2, Some(()), None).await?;

    // Purge will keep at least two snapshots.
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot_files = fuse_table.list_snapshot_files().await?;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    fuse_table
        .do_purge(&table_ctx, snapshot_files, None, true, false)
        .await?;
    check_data_dir(&fixture, case_name, 1, 1, 1, 1, 1, Some(()), Some(())).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_snapshot_analyze_and_truncate() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let case_name = "test_fuse_snapshot_analyze_and_truncate";

    // insert some data
    do_insertions(&fixture).await?;

    // analyze the table
    {
        let qry = format!("Analyze table {}.{}", db, tbl);

        fixture.execute_command(&qry).await?;

        check_data_dir(&fixture, case_name, 3, 1, 2, 2, 2, None, Some(())).await?;
    }

    // truncate table
    {
        let qry = format!("Truncate table {}.{}", db, tbl);
        let r = fixture.execute_command(&qry).await;
        assert!(r.is_ok());
    }

    // optimize after truncate table, ts file location will become None
    {
        let ctx = fixture.new_query_ctx().await?;
        let catalog = ctx
            .get_catalog(fixture.default_catalog_name().as_str())
            .await?;
        let table = catalog.get_table(&ctx.get_tenant(), &db, &tbl).await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot_opt = fuse_table.read_table_snapshot().await?;
        assert!(snapshot_opt.is_some());
        assert!(snapshot_opt.unwrap().table_statistics_location.is_none());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_snapshot_analyze_purge() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;
    let case_name = "analyze_statistic_purge";
    do_insertions(&fixture).await?;

    // optimize statistics three times
    for i in 0..3 {
        analyze_table(&fixture).await?;
        check_data_dir(&fixture, case_name, 3 + i, 1 + i, 2, 2, 2, Some(()), None).await?;
    }

    // Purge will keep at least two snapshots.
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot_files = fuse_table.list_snapshot_files().await?;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    fuse_table
        .do_purge(&table_ctx, snapshot_files, None, true, false)
        .await?;
    check_data_dir(&fixture, case_name, 1, 1, 1, 1, 1, Some(()), Some(())).await?;

    Ok(())
}
