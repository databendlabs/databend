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
use common_context::IOContext;
use common_dal::read_obj;
use common_datablocks::DataBlock;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::series::Series;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use common_infallible::Mutex;
use common_meta_types::TableMeta;
use common_planners::col;
use common_planners::lit;
use common_planners::CreateTablePlan;
use common_planners::Extras;
use common_planners::InsertIntoPlan;

use crate::catalogs::Catalog;
use crate::datasources::table::fuse::index::min_max::range_filter;
use crate::datasources::table::fuse::table_test_fixture::TestFixture;
use crate::datasources::table::fuse::util::TBL_OPT_KEY_SNAPSHOT_LOC;

#[tokio::test]
async fn test_min_max_index() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let test_tbl_name = "test_index_helper";
    let test_schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::UInt64, false),
        DataField::new("b", DataType::UInt64, false),
    ]);

    // create test table
    let crate_table_plan = CreateTablePlan {
        if_not_exists: false,
        db: fixture.default_db(),
        table: test_tbl_name.to_string(),
        table_meta: TableMeta {
            schema: test_schema.clone(),
            engine: "FUSE".to_string(),
            options: Default::default(),
        },
    };

    let catalog = ctx.get_catalog();
    catalog.create_table(crate_table_plan).await?;

    // get table
    let table = catalog
        .get_table(fixture.default_db().as_str(), test_tbl_name)
        .await?;

    // prepare test blocks
    let num = 10;
    let blocks = (0..num)
        .into_iter()
        .map(|idx| {
            DataBlock::create_by_array(test_schema.clone(), vec![
                Series::new(vec![idx + 1, idx + 2, idx + 3]),
                Series::new(vec![idx * num + 1, idx * num + 2, idx * num + 3]),
            ])
        })
        .collect::<Vec<_>>();

    let insert_into_plan = InsertIntoPlan {
        db_name: fixture.default_db(),
        tbl_name: test_tbl_name.to_string(),
        tbl_id: table.get_id(),
        schema: test_schema.clone(),
        input_stream: Arc::new(Mutex::new(Some(Box::pin(futures::stream::iter(blocks))))),
    };
    let io_ctx = Arc::new(ctx.get_single_node_table_io_context()?);
    let da = io_ctx.get_data_accessor()?;
    table.append_data(io_ctx.clone(), insert_into_plan).await?;

    // get the latest tbl
    let table = catalog
        .get_table(fixture.default_db().as_str(), test_tbl_name)
        .await?;

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(TBL_OPT_KEY_SNAPSHOT_LOC)
        .unwrap();
    let snapshot = read_obj(da.clone(), snapshot_loc.clone()).await?;

    // no pruning
    let push_downs = None;
    let blocks = range_filter(
        &snapshot,
        table.get_table_info().schema(),
        push_downs,
        da.clone(),
    )
    .await?;
    let rows: u64 = blocks.iter().map(|b| b.row_count).sum();
    assert_eq!(rows, num * 3u64);
    assert_eq!(10, blocks.len());

    // fully pruned
    let mut extra = Extras::default();
    let pred = col("a").gt(lit(30));
    extra.filters = vec![pred];

    let blocks = range_filter(
        &snapshot,
        table.get_table_info().schema(),
        Some(extra),
        da.clone(),
    )
    .await?;
    assert_eq!(0, blocks.len());

    // one block pruned
    let mut extra = Extras::default();
    let pred = col("a").gt(lit(3)).and(col("b").gt(lit(3)));
    extra.filters = vec![pred];

    let blocks = range_filter(&snapshot, table.get_table_info().schema(), Some(extra), da).await?;
    assert_eq!(num - 1, blocks.len() as u64);

    Ok(())
}
