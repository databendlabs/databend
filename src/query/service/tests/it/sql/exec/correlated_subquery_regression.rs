// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

async fn execute_query_rows(sql: &str) -> Result<usize> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = interpreter.execute(ctx).await?;
    let blocks: Vec<DataBlock> = stream.try_collect().await?;
    Ok(DataBlock::concat(&blocks)?.num_rows())
}

#[tokio::test(flavor = "multi_thread")]
async fn correlated_exists_subquery_over_union_regression() -> anyhow::Result<()> {
    let sql = r"
        SELECT *
        FROM (VALUES (1)) t(f1)
        WHERE EXISTS (
          SELECT 1
          UNION
          SELECT 2 WHERE f1 = 1
        );
    ";

    let rows = execute_query_rows(sql).await?;
    assert_eq!(rows, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn correlated_exists_subquery_over_union_all_regression() -> anyhow::Result<()> {
    let sql = r"
        SELECT *
        FROM (VALUES (1)) t(f1)
        WHERE EXISTS (
          SELECT 1
          UNION ALL
          SELECT 2 WHERE f1 = 1
        );
    ";

    let rows = execute_query_rows(sql).await?;
    assert_eq!(rows, 1);

    Ok(())
}
