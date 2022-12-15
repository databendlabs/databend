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

use common_base::base::tokio;
use common_exception::Result;
use common_storages_factory::Table;
use common_storages_fuse::FuseTable;
use common_storages_fuse::TableContext;

use crate::storages::fuse::table_test_fixture::check_data_dir;
use crate::storages::fuse::table_test_fixture::execute_command;
use crate::storages::fuse::table_test_fixture::TestFixture;
use crate::storages::fuse::utils::do_insertions;
use crate::storages::fuse::utils::do_purge_test;
use crate::storages::fuse::utils::TestTableOperation;

#[tokio::test]
async fn test_fuse_snapshot_analyze() -> Result<()> {
    do_purge_test(
        "test_fuse_snapshot_analyze",
        TestTableOperation::Analyze,
        3,
        1,
        2,
        2,
        2,
        // After compact, all the count will become 1
        Some((1, 1, 1, 1, 1)),
    )
    .await
}

#[tokio::test]
async fn test_fuse_snapshot_analyze_and_truncate() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let case_name = "test_fuse_snapshot_analyze_and_truncate";

    // insert some data
    do_insertions(&fixture).await?;

    // analyze the table
    {
        let qry = format!("Analyze table {}.{}", db, tbl);

        let ctx = fixture.ctx();
        execute_command(ctx, &qry).await?;

        check_data_dir(&fixture, case_name, 3, 1, 2, 2, 2, None, Some(())).await?;
    }

    // truncate table
    {
        let ctx = fixture.ctx();
        let catalog = ctx.get_catalog(fixture.default_catalog_name().as_str())?;
        let table = catalog
            .get_table(ctx.get_tenant().as_str(), &db, &tbl)
            .await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table.truncate(ctx, false).await?;
    }

    // optimize after truncate table, ts file location will become None
    {
        let ctx = fixture.ctx();
        let catalog = ctx.get_catalog(fixture.default_catalog_name().as_str())?;
        let table = catalog
            .get_table(ctx.get_tenant().as_str(), &db, &tbl)
            .await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot_opt = fuse_table.read_table_snapshot().await?;
        assert!(snapshot_opt.is_some());
        assert!(snapshot_opt.unwrap().table_statistics_location.is_none());
    }

    Ok(())
}

#[tokio::test]
async fn test_fuse_snapshot_analyze_purge() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let case_name = "analyze_statistic_purge";
    do_insertions(&fixture).await?;

    // optimize statistics twice
    for i in 0..1 {
        let qry = format!("Analyze table {}.{}", db, tbl);

        let ctx = fixture.ctx();
        execute_command(ctx, &qry).await?;

        check_data_dir(&fixture, case_name, 3, 1 + i, 2, 2, 2, Some(()), None).await?;
    }

    // After compact, all the count will become 1
    let qry = format!("optimize table {}.{} all", db, tbl);
    execute_command(fixture.ctx().clone(), &qry).await?;

    check_data_dir(&fixture, case_name, 1, 1, 1, 1, 1, Some(()), Some(())).await?;

    Ok(())
}
