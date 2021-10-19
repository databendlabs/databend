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
//

use std::sync::Arc;

use common_base::tokio;
use common_exception::Result;
use common_planners::TruncateTablePlan;
use futures::TryStreamExt;

use crate::catalogs::Catalog;
use crate::catalogs::ToReadDataSourcePlan;
use crate::datasources::table::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_table_simple_case() -> Result<()> {
    let fixture = TestFixture::new();
    let ctx = fixture.ctx();

    // create test table
    let crate_table_plan = TestFixture::default_crate_table_plan();
    let db = ctx
        .get_catalog()
        .get_database(TestFixture::default_db().as_str())?;
    db.create_table(crate_table_plan)?;

    // get table
    let table = db.get_table(TestFixture::default_table().as_str())?;

    // insert 10 blocks
    let io_ctx = Arc::new(ctx.get_single_node_table_io_context()?);
    let insert_into_plan = TestFixture::insert_plan_for_default_table(table.as_ref(), 10);
    table.append_data(io_ctx.clone(), insert_into_plan).await?;

    // get the latest tbl
    let prev_version = table.get_table_info().version;
    let table = db.get_table(TestFixture::default_table().as_str())?;
    assert_ne!(prev_version, table.get_table_info().version);

    let (stats, parts) = table.read_partitions(io_ctx.clone(), None, None)?;
    assert_eq!(parts.len(), 10);
    assert_eq!(stats.read_rows, 10 * 3);

    // inject partitions to current ctx
    ctx.try_set_partitions(parts)?;

    let stream = table.read(io_ctx, &None).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(rows, 10 * 3);

    let expected = vec![
        "+----+", "| id |", "+----+", "| 1  |", "| 1  |", "| 1  |", "| 1  |", "| 1  |", "| 1  |",
        "| 1  |", "| 1  |", "| 1  |", "| 1  |", "| 2  |", "| 2  |", "| 2  |", "| 2  |", "| 2  |",
        "| 2  |", "| 2  |", "| 2  |", "| 2  |", "| 2  |", "| 3  |", "| 3  |", "| 3  |", "| 3  |",
        "| 3  |", "| 3  |", "| 3  |", "| 3  |", "| 3  |", "| 3  |", "+----+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());

    Ok(())
}

#[tokio::test]
async fn test_fuse_table_truncate() -> Result<()> {
    let fixture = TestFixture::new();
    let ctx = fixture.ctx();

    let crate_table_plan = TestFixture::default_crate_table_plan();
    let db = ctx
        .get_catalog()
        .get_database(TestFixture::default_db().as_str())?;
    db.create_table(crate_table_plan)?;
    let table = db.get_table(TestFixture::default_table().as_str())?;

    let io_ctx = Arc::new(ctx.get_single_node_table_io_context()?);
    let truncate_plan = TruncateTablePlan {
        db: "".to_string(),
        table: "".to_string(),
    };

    // 1. truncate empty table
    let prev_version = table.get_table_info().version;
    let r = table.truncate(io_ctx.clone(), truncate_plan.clone()).await;
    let table = db.get_table(TestFixture::default_table().as_str())?;
    // no side effects
    assert_eq!(prev_version, table.get_table_info().version);
    assert!(r.is_ok());

    // 2. truncate table which has data
    let insert_into_plan = TestFixture::insert_plan_for_default_table(table.as_ref(), 10);
    table.append_data(io_ctx.clone(), insert_into_plan).await?;
    let source_plan = table.read_plan(
        io_ctx.clone(),
        None,
        Some(ctx.get_settings().get_max_threads()? as usize),
    )?;

    // get the latest tbl
    let prev_version = table.get_table_info().version;
    let table = db.get_table(TestFixture::default_table().as_str())?;
    assert_ne!(prev_version, table.get_table_info().version);

    // ensure data ingested
    let (stats, parts) =
        table.read_partitions(io_ctx.clone(), source_plan.push_downs.clone(), None)?;
    assert_eq!(parts.len(), 10);
    assert_eq!(stats.read_rows, 10 * 3);

    // truncate
    let r = table.truncate(io_ctx.clone(), truncate_plan).await;
    assert!(r.is_ok());

    // get the latest tbl
    let prev_version = table.get_table_info().version;
    let table = db.get_table(TestFixture::default_table().as_str())?;
    assert_ne!(prev_version, table.get_table_info().version);
    let (stats, parts) =
        table.read_partitions(io_ctx.clone(), source_plan.push_downs.clone(), None)?;
    // cleared?
    assert_eq!(parts.len(), 0);
    assert_eq!(stats.read_rows, 0);

    Ok(())
}
