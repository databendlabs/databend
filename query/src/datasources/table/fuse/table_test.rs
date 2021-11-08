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

use std::collections::HashMap;
use std::sync::Arc;

use common_base::tokio;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Extras;
use common_planners::ReadDataSourcePlan;
use common_planners::TruncateTablePlan;
use futures::TryStreamExt;

use crate::catalogs::Catalog;
use crate::catalogs::ToReadDataSourcePlan;
use crate::datasources::table::fuse::table_test_fixture::TestFixture;
use crate::datasources::table::fuse::BlockLocation;
use crate::datasources::table::fuse::BlockMeta;
use crate::datasources::table::fuse::ColStats;
use crate::datasources::table::fuse::FuseTable;

#[tokio::test]
async fn test_fuse_table_simple_case() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // create test table
    let crate_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(crate_table_plan).await?;

    // get table
    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;

    // insert 10 blocks
    let num_blocks = 5;
    let io_ctx = Arc::new(ctx.get_cluster_table_io_context()?);
    let insert_into_plan = fixture.insert_plan_of_table(table.as_ref(), num_blocks);
    table.append_data(io_ctx.clone(), insert_into_plan).await?;

    // get the latest tbl
    let prev_version = table.get_table_info().ident.version;
    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;
    assert_ne!(prev_version, table.get_table_info().ident.version);

    let (stats, parts) = table.read_partitions(io_ctx.clone(), None)?;
    assert_eq!(parts.len(), num_blocks as usize);
    assert_eq!(stats.read_rows, num_blocks as usize * 3);

    // inject partitions to current ctx
    ctx.try_set_partitions(parts)?;

    let stream = table
        .read(io_ctx, &ReadDataSourcePlan {
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

    Ok(())
}

#[tokio::test]
async fn test_fuse_table_truncate() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let crate_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(crate_table_plan).await?;

    let table = catalog
        .get_table(
            fixture.default_db().as_str(),
            fixture.default_table().as_str(),
        )
        .await?;

    let io_ctx = Arc::new(ctx.get_cluster_table_io_context()?);
    let truncate_plan = TruncateTablePlan {
        db: "".to_string(),
        table: "".to_string(),
    };

    // 1. truncate empty table
    let prev_version = table.get_table_info().ident.version;
    let r = table.truncate(io_ctx.clone(), truncate_plan.clone()).await;
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
    let insert_into_plan = fixture.insert_plan_of_table(table.as_ref(), 10);
    table.append_data(io_ctx.clone(), insert_into_plan).await?;
    let source_plan = table.read_plan(io_ctx.clone(), None)?;

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
    let (stats, parts) = table.read_partitions(io_ctx.clone(), source_plan.push_downs.clone())?;
    assert_eq!(parts.len(), 10);
    assert_eq!(stats.read_rows, 10 * 3);

    // truncate
    let r = table.truncate(io_ctx.clone(), truncate_plan).await;
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
    let (stats, parts) = table.read_partitions(io_ctx.clone(), source_plan.push_downs.clone())?;
    // cleared?
    assert_eq!(parts.len(), 0);
    assert_eq!(stats.read_rows, 0);

    Ok(())
}

#[test]
fn test_fuse_table_to_partitions_stats_with_col_pruning() -> Result<()> {
    // setup
    let num_of_col = 10;
    let num_of_block = 5;

    let col_stats_gen = |col_size| ColStats {
        min: DataValue::Int8(Some(1)),
        max: DataValue::Int8(Some(2)),
        null_count: 0,
        in_memory_size: col_size as u64,
    };

    let cols_stats = (0..num_of_col)
        .into_iter()
        .map(|col_id| (col_id as u32, col_stats_gen(col_id)))
        .collect::<HashMap<_, _>>();

    let block_meta = BlockMeta {
        row_count: 0,
        block_size: cols_stats
            .iter()
            .map(|(_, col_stats)| col_stats.in_memory_size)
            .sum(),
        col_stats: cols_stats.clone(),
        location: BlockLocation {
            location: "".to_string(),
            meta_size: 0,
        },
    };

    let blocks_metas = (0..num_of_block)
        .into_iter()
        .map(|_| block_meta.clone())
        .collect::<Vec<_>>();

    // CASE I:  no projection
    let (s, _) = FuseTable::to_partitions(&blocks_metas, None);
    let expected_block_size: u64 = cols_stats
        .iter()
        .map(|(_, col_stats)| col_stats.in_memory_size)
        .sum();
    assert_eq!(expected_block_size * num_of_block, s.read_bytes as u64);

    // CASE II: col pruning
    // projection which keeps the odd ones
    let proj = (0..num_of_col)
        .into_iter()
        .filter(|v| v & 1 != 0)
        .collect::<Vec<usize>>();

    // for each block, the block size we expects (after pruning)
    let expected_block_size: u64 = cols_stats
        .iter()
        .filter(|(cid, _)| proj.contains(&(**cid as usize)))
        .map(|(_, col_stats)| col_stats.in_memory_size)
        .sum();

    // kick off
    let push_down = Some(Extras {
        projection: Some(proj),
        filters: vec![],
        limit: None,
        order_by: vec![],
    });
    let (stats, _) = FuseTable::to_partitions(&blocks_metas, push_down);
    assert_eq!(expected_block_size * num_of_block, stats.read_bytes as u64);
    Ok(())
}
