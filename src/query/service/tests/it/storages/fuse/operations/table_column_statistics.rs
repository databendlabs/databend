//  Copyright 2022 Datafuse Labs.
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

use std::collections::HashMap;
use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_datablocks::DataBlock;
use common_datablocks::SendableDataBlockStream;
use common_exception::Result;
use common_storages_fuse::FuseTable;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sql::plans::Plan;
use databend_query::sql::Planner;
use futures_util::TryStreamExt;

use crate::storages::fuse::table_test_fixture::execute_command;
use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_table_modify_column_ndv_statistics() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let create_tbl_command = "create table t(c int)";
    execute_command(ctx.clone(), create_tbl_command).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 3;
    append_rows(ctx.clone(), num_inserts).await?;

    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(num_inserts, check_count(stream).await? as usize);

    let expected = Some(HashMap::from([(0, num_inserts as u64)]));
    let result = get_column_ndv_statistics(table.clone(), ctx.clone()).await?;
    assert_eq!(result, expected);

    // append the same values again, and ndv does changed.
    append_rows(ctx.clone(), num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(num_inserts * 2, check_count(stream).await? as usize);

    let result = get_column_ndv_statistics(table.clone(), ctx.clone()).await?;
    assert_eq!(result, expected);

    // delete
    let query = "delete from default.t where c=1";
    let mut planner = Planner::new(ctx.clone());
    let (plan, _, _) = planner.plan_sql(query).await?;
    if let Plan::Delete(delete) = plan {
        table
            .delete(ctx.clone(), &delete.projection, &delete.selection)
            .await?;
    }

    // check count
    let expected = Some(HashMap::from([(0, num_inserts as u64 - 1)]));
    let result = get_column_ndv_statistics(table.clone(), ctx.clone()).await?;
    assert_eq!(result, expected);

    Ok(())
}

async fn get_column_ndv_statistics(
    table: Arc<dyn Table>,
    ctx: Arc<QueryContext>,
) -> Result<Option<HashMap<u32, u64>>> {
    let latest = table.refresh(ctx.as_ref()).await?;
    let latest_fuse_table = FuseTable::try_from_table(latest.as_ref())?;
    let snapshot = latest_fuse_table.read_table_snapshot().await?;
    match snapshot {
        Some(snapshot) => Ok(snapshot.column_counts.clone()),
        None => Ok(None),
    }
}

async fn append_rows(ctx: Arc<QueryContext>, n: usize) -> Result<()> {
    for i in 0..n {
        let qry = format!("insert into t values({})", i);
        execute_command(ctx.clone(), &qry).await?;
    }
    Ok(())
}

async fn check_count(result_stream: SendableDataBlockStream) -> Result<u64> {
    let blocks: Vec<DataBlock> = result_stream.try_collect().await?;
    let mut count: u64 = 0;
    for block in blocks {
        count += block.column(0).get_u64(0)?;
    }
    Ok(count)
}
