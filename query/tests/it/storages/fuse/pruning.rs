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
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::CreateTableReq;
use common_meta_types::TableMeta;
use common_planners::add;
use common_planners::col;
use common_planners::lit;
use common_planners::sub;
use common_planners::Extras;
use databend_query::catalogs::Catalog;
use databend_query::sessions::QueryContext;
use databend_query::storages::fuse::io::MetaReaders;
use databend_query::storages::fuse::meta::BlockMeta;
use databend_query::storages::fuse::meta::TableSnapshot;
use databend_query::storages::fuse::pruning::BlockPruner;
use databend_query::storages::fuse::TBL_OPT_KEY_BLOCK_PER_SEGMENT;
use databend_query::storages::fuse::TBL_OPT_KEY_ROW_PER_BLOCK;
use databend_query::storages::fuse::TBL_OPT_KEY_SNAPSHOT_LOC;
use futures::TryStreamExt;

use crate::storages::fuse::table_test_fixture::TestFixture;

async fn apply_block_pruning(
    table_snapshot: &TableSnapshot,
    schema: DataSchemaRef,
    push_down: &Option<Extras>,
    ctx: Arc<QueryContext>,
) -> Result<Vec<BlockMeta>> {
    BlockPruner::new(table_snapshot)
        .apply(schema, push_down, ctx.as_ref())
        .await
}

#[tokio::test]
async fn test_block_pruner() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let test_tbl_name = "test_index_helper";
    let test_schema = DataSchemaRefExt::create(vec![
        DataField::new("a", u64::to_data_type()),
        DataField::new("b", u64::to_data_type()),
    ]);

    let num_blocks = 10;
    let row_per_block = 10;
    let num_blocks_opt = row_per_block.to_string();

    // create test table
    let crate_table_plan = CreateTableReq {
        if_not_exists: false,
        tenant: fixture.default_tenant(),
        db: fixture.default_db_name(),
        table: test_tbl_name.to_string(),
        table_meta: TableMeta {
            schema: test_schema.clone(),
            engine: "FUSE".to_string(),
            options: [
                (TBL_OPT_KEY_ROW_PER_BLOCK.to_owned(), num_blocks_opt),
                // for the convenience of testing, let one seegment contains one block
                (TBL_OPT_KEY_BLOCK_PER_SEGMENT.to_owned(), "1".to_owned()),
            ]
            .into(),
            ..Default::default()
        },
    };

    let catalog = ctx.get_catalog();
    catalog.create_table(crate_table_plan).await?;

    // get table
    let table = catalog
        .get_table(
            fixture.default_tenant().as_str(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let gen_col =
        |value, rows| Series::from_data(std::iter::repeat(value).take(rows).collect::<Vec<u64>>());

    // prepare test blocks
    // - there will be `num_blocks` blocks, for each block, it comprises of `row_per_block` rows,
    //    in our case, there will be 10 blocks, and 10 rows for each block
    let blocks = (0..num_blocks)
        .into_iter()
        .map(|idx| {
            Ok(DataBlock::create(test_schema.clone(), vec![
                // value of column a always equals  1
                gen_col(1, row_per_block),
                // for column b
                // - for all block `B` in blocks, whose index is `i`
                // - for all row in `B`, value of column b  equals `i`
                gen_col(idx as u64, row_per_block),
            ]))
        })
        .collect::<Vec<_>>();

    let stream = Box::pin(futures::stream::iter(blocks));
    let r = table.append_data(ctx.clone(), stream).await?;
    table
        .commit_insertion(ctx.clone(), r.try_collect().await?, false)
        .await?;

    // get the latest tbl
    let table = catalog
        .get_table(
            fixture.default_tenant().as_str(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(TBL_OPT_KEY_SNAPSHOT_LOC)
        .unwrap();

    let reader = MetaReaders::table_snapshot_reader(ctx.as_ref());
    let snapshot = reader.read(snapshot_loc.as_str()).await?;

    // nothing will be pruned
    let push_downs = None;
    let blocks = apply_block_pruning(
        &snapshot,
        table.get_table_info().schema(),
        &push_downs,
        ctx.clone(),
    )
    .await?;
    let rows = blocks.iter().map(|b| b.row_count as usize).sum::<usize>();
    assert_eq!(rows, num_blocks * row_per_block);
    assert_eq!(num_blocks, blocks.len());

    // fully pruned
    let mut extra = Extras::default();
    // max value of col a is
    let pred = col("a").gt(lit(30u64));
    extra.filters = vec![pred];

    let blocks = apply_block_pruning(
        &snapshot,
        table.get_table_info().schema(),
        &Some(extra),
        ctx.clone(),
    )
    .await?;
    assert_eq!(0, blocks.len());

    // some blocks pruned
    let mut extra = Extras::default();
    let max_val_of_b = 6u64;
    let pred = col("a").gt(lit(0u64)).and(col("b").gt(lit(max_val_of_b)));
    extra.filters = vec![pred];

    let blocks = apply_block_pruning(
        &snapshot,
        table.get_table_info().schema(),
        &Some(extra),
        ctx.clone(),
    )
    .await?;

    assert_eq!((num_blocks - max_val_of_b as usize - 1), blocks.len());

    Ok(())
}

#[tokio::test]
async fn test_block_pruner_monotonic() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let test_tbl_name = "test_index_helper";
    let test_schema = DataSchemaRefExt::create(vec![
        DataField::new("a", u64::to_data_type()),
        DataField::new("b", u64::to_data_type()),
    ]);

    let row_per_block = 3;
    let num_blocks_opt = row_per_block.to_string();

    // create test table
    let crate_table_plan = CreateTableReq {
        if_not_exists: false,
        tenant: fixture.default_tenant(),
        db: fixture.default_db_name(),
        table: test_tbl_name.to_string(),
        table_meta: TableMeta {
            schema: test_schema.clone(),
            engine: "FUSE".to_string(),
            options: [
                (TBL_OPT_KEY_ROW_PER_BLOCK.to_owned(), num_blocks_opt),
                // for the convenience of testing, let one seegment contains one block
                (TBL_OPT_KEY_BLOCK_PER_SEGMENT.to_owned(), "1".to_owned()),
            ]
            .into(),
            ..Default::default()
        },
    };

    let catalog = ctx.get_catalog();
    catalog.create_table(crate_table_plan).await?;

    // get table
    let table = catalog
        .get_table(
            fixture.default_tenant().as_str(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let blocks = vec![
        Ok(DataBlock::create(test_schema.clone(), vec![
            Series::from_data(vec![1u64, 2, 3]),
            Series::from_data(vec![11u64, 12, 13]),
        ])),
        Ok(DataBlock::create(test_schema.clone(), vec![
            Series::from_data(vec![4u64, 5, 6]),
            Series::from_data(vec![21u64, 22, 23]),
        ])),
        Ok(DataBlock::create(test_schema, vec![
            Series::from_data(vec![7u64, 8, 9]),
            Series::from_data(vec![31u64, 32, 33]),
        ])),
    ];

    let stream = Box::pin(futures::stream::iter(blocks));
    let r = table.append_data(ctx.clone(), stream).await?;
    table
        .commit_insertion(ctx.clone(), r.try_collect().await?, false)
        .await?;

    // get the latest tbl
    let table = catalog
        .get_table(
            fixture.default_tenant().as_str(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(TBL_OPT_KEY_SNAPSHOT_LOC)
        .unwrap();

    let reader = MetaReaders::table_snapshot_reader(ctx.as_ref());
    let snapshot = reader.read(snapshot_loc.as_str()).await?;

    // a + b > 20; some blocks pruned
    let mut extra = Extras::default();
    let pred = add(col("a"), col("b")).gt(lit(20u64));
    extra.filters = vec![pred];

    let blocks = apply_block_pruning(
        &snapshot,
        table.get_table_info().schema(),
        &Some(extra),
        ctx.clone(),
    )
    .await?;

    assert_eq!(2, blocks.len());

    // b - a < 20; nothing will be pruned.
    let mut extra = Extras::default();
    let pred = sub(col("b"), col("a")).lt(lit(20u64));
    extra.filters = vec![pred];

    let blocks = apply_block_pruning(
        &snapshot,
        table.get_table_info().schema(),
        &Some(extra),
        ctx.clone(),
    )
    .await?;

    assert_eq!(3, blocks.len());

    Ok(())
}
