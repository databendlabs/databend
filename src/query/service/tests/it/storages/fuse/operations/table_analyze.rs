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

use std::collections::HashMap;
use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::table::Table;
use common_exception::Result;
use common_expression::types::number::NumberScalar;
use common_expression::DataBlock;
use common_expression::ScalarRef;
use common_expression::SendableDataBlockStream;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sql::plans::Plan;
use databend_query::sql::Planner;
use databend_query::test_kits::table_test_fixture::execute_command;
use databend_query::test_kits::table_test_fixture::execute_query;
use databend_query::test_kits::table_test_fixture::TestFixture;
use futures_util::TryStreamExt;

use crate::storages::fuse::operations::mutation::do_deletion;

#[tokio::test(flavor = "multi_thread")]
async fn test_table_modify_column_ndv_statistics() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let create_tbl_command = "create table t(c int)";
    execute_command(ctx.clone(), create_tbl_command).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 3;
    append_rows(ctx.clone(), num_inserts).await?;
    let statistics_sql = "analyze table default.t";
    execute_command(ctx.clone(), statistics_sql).await?;

    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(num_inserts, check_count(stream).await? as usize);

    let expected = HashMap::from([(0, num_inserts as u64)]);
    check_column_ndv_statistics(table.clone(), expected.clone()).await?;

    // append the same values again, and ndv does changed.
    append_rows(ctx.clone(), num_inserts).await?;
    execute_command(ctx.clone(), statistics_sql).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(num_inserts * 2, check_count(stream).await? as usize);

    check_column_ndv_statistics(table.clone(), expected.clone()).await?;

    // delete
    let query = "delete from default.t where c=1";
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(query).await?;
    if let Plan::Delete(delete) = plan {
        do_deletion(ctx.clone(), table.clone(), *delete).await?;
    }
    execute_command(ctx.clone(), statistics_sql).await?;

    // check count: delete not affect counts
    check_column_ndv_statistics(table.clone(), expected).await?;

    Ok(())
}

async fn check_column_ndv_statistics(
    table: Arc<dyn Table>,
    expected: HashMap<u32, u64>,
) -> Result<()> {
    let provider = table.column_statistics_provider().await?;

    for (i, num) in expected.iter() {
        let stat = provider.column_statistics(*i);
        assert!(stat.is_some());

        assert_eq!(stat.unwrap().number_of_distinct_values, *num);
    }

    Ok(())
}

async fn check_count(result_stream: SendableDataBlockStream) -> Result<u64> {
    let blocks: Vec<DataBlock> = result_stream.try_collect().await?;
    let mut count: u64 = 0;
    for block in blocks {
        let value = &block.get_by_offset(0).value;
        let value = value.as_ref();
        let value = unsafe { value.index_unchecked(0) };
        if let ScalarRef::Number(NumberScalar::UInt64(v)) = value {
            count += v;
        }
    }
    Ok(count)
}

async fn append_rows(ctx: Arc<QueryContext>, n: usize) -> Result<()> {
    for i in 0..n {
        let qry = format!("insert into t values({})", i);
        execute_command(ctx.clone(), &qry).await?;
    }
    Ok(())
}
