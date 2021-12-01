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

use common_base::tokio;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_planners::TruncateTablePlan;
use futures::TryStreamExt;

use crate::catalogs::Catalog;
use crate::interpreters::InterpreterFactory;
use crate::sql::PlanParser;
use crate::storages::fuse::table_test_fixture::TestFixture;
use crate::storages::ToReadDataSourcePlan;

#[tokio::test]
async fn test_fuse_table_simple_case() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // create test table
    let create_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(create_table_plan.into()).await?;

    // get table
    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;

    // insert 5 blocks
    let num_blocks = 5;
    let stream = Box::pin(futures::stream::iter(TestFixture::gen_block_stream(
        num_blocks, 1,
    )));

    let r = table.append_data(ctx.clone(), stream).await?;
    table
        .commit(ctx.clone(), r.try_collect().await?, false)
        .await?;

    // get the latest tbl
    let prev_version = table.get_table_info().ident.version;
    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;
    assert_ne!(prev_version, table.get_table_info().ident.version);

    let (stats, parts) = table.read_partitions(ctx.clone(), None).await?;
    assert_eq!(parts.len(), num_blocks as usize);
    assert_eq!(stats.read_rows, num_blocks as usize * 3);

    // inject partitions to current ctx
    ctx.try_set_partitions(parts)?;

    let stream = table
        .read(ctx.clone(), &ReadDataSourcePlan {
            table_info: Default::default(),
            scan_fields: None,
            parts: Default::default(),
            statistics: Default::default(),
            description: "".to_string(),
            tbl_args: None,
            push_downs: None,
        })
        .await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(rows, num_blocks as usize * 3);

    let expected = vec![
        "+----+", //
        "| id |", //
        "+----+", //
        "| 1  |", //
        "| 1  |", //
        "| 1  |", //
        "| 1  |", //
        "| 1  |", //
        "| 2  |", //
        "| 2  |", //
        "| 2  |", //
        "| 2  |", //
        "| 2  |", //
        "| 3  |", //
        "| 3  |", //
        "| 3  |", //
        "| 3  |", //
        "| 3  |", //
        "+----+", //
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());

    // test commit with overwrite

    // insert overwrite 5 blocks
    let num_blocks = 5;
    let stream = Box::pin(futures::stream::iter(TestFixture::gen_block_stream(
        num_blocks, 4,
    )));

    let r = table.append_data(ctx.clone(), stream).await?;
    table
        .commit(ctx.clone(), r.try_collect().await?, true)
        .await?;

    // get the latest tbl
    let prev_version = table.get_table_info().ident.version;
    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;
    assert_ne!(prev_version, table.get_table_info().ident.version);

    let (stats, parts) = table.read_partitions(ctx.clone(), None).await?;
    assert_eq!(parts.len(), num_blocks as usize);
    assert_eq!(stats.read_rows, num_blocks as usize * 3);

    // inject partitions to current ctx
    ctx.try_set_partitions(parts)?;

    let stream = table
        .read(ctx.clone(), &ReadDataSourcePlan {
            table_info: Default::default(),
            scan_fields: None,
            parts: Default::default(),
            statistics: Default::default(),
            description: "".to_string(),
            tbl_args: None,
            push_downs: None,
        })
        .await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(rows, num_blocks as usize * 3);

    let expected = vec![
        "+----+", //
        "| id |", //
        "+----+", //
        "| 4  |", //
        "| 4  |", //
        "| 4  |", //
        "| 4  |", //
        "| 4  |", //
        "| 5  |", //
        "| 5  |", //
        "| 5  |", //
        "| 5  |", //
        "| 5  |", //
        "| 6  |", //
        "| 6  |", //
        "| 6  |", //
        "| 6  |", //
        "| 6  |", //
        "+----+", //
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());

    Ok(())
}

#[tokio::test]
async fn test_fuse_table_truncate() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let create_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(create_table_plan.into()).await?;

    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;

    let truncate_plan = TruncateTablePlan {
        db: "".to_string(),
        table: "".to_string(),
    };

    // 1. truncate empty table
    let prev_version = table.get_table_info().ident.version;
    let r = table.truncate(ctx.clone(), truncate_plan.clone()).await;
    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;
    // no side effects
    assert_eq!(prev_version, table.get_table_info().ident.version);
    assert!(r.is_ok());

    // 2. truncate table which has data
    let num_blocks = 10;
    let stream = Box::pin(futures::stream::iter(TestFixture::gen_block_stream(
        num_blocks, 1,
    )));

    let r = table.append_data(ctx.clone(), stream).await?;
    table
        .commit(ctx.clone(), r.try_collect().await?, false)
        .await?;
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    // get the latest tbl
    let prev_version = table.get_table_info().ident.version;
    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;
    assert_ne!(prev_version, table.get_table_info().ident.version);

    // ensure data ingested
    let (stats, parts) = table
        .read_partitions(ctx.clone(), source_plan.push_downs.clone())
        .await?;
    assert_eq!(parts.len(), 10);
    assert_eq!(stats.read_rows, 10 * 3);

    // truncate
    let r = table.truncate(ctx.clone(), truncate_plan).await;
    assert!(r.is_ok());

    // get the latest tbl
    let prev_version = table.get_table_info().ident.version;
    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;
    assert_ne!(prev_version, table.get_table_info().ident.version);
    let (stats, parts) = table
        .read_partitions(ctx.clone(), source_plan.push_downs.clone())
        .await?;
    // cleared?
    assert_eq!(parts.len(), 0);
    assert_eq!(stats.read_rows, 0);

    Ok(())
}

#[tokio::test]
async fn test_fuse_table_compact() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // create test table
    let create_table_plan = fixture.default_crate_table_plan();
    let tbl_name = create_table_plan.table.clone();
    let db_name = create_table_plan.db.clone();
    let catalog = ctx.get_catalog();
    catalog.create_table(create_table_plan.into()).await?;

    // get table
    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;

    // insert 5 blocks
    let num_blocks = 5;
    let stream = Box::pin(futures::stream::iter(TestFixture::gen_block_stream(
        num_blocks, 1,
    )));

    let r = table.append_data(ctx.clone(), stream).await?;
    table
        .commit(ctx.clone(), r.try_collect().await?, false)
        .await?;

    // do compact
    let query = format!("compact table {}.{}", db_name, tbl_name);
    let plan = PlanParser::parse(&query, ctx.clone()).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;
    let data_stream = interpreter.execute(None).await?;
    let _ = data_stream.try_collect::<Vec<_>>();

    // verify compaction
    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;
    let (stats, parts) = table.read_partitions(ctx.clone(), None).await?;
    // blocks are so tiny, they should be compacted into one
    assert_eq!(parts.len(), 1);
    assert_eq!(stats.read_rows, num_blocks as usize * 3);

    Ok(())
}
