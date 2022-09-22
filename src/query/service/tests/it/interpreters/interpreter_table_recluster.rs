// Copyright 2022 Datafuse Labs.
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

use common_base::base::tokio;
use common_exception::Result;
use databend_query::interpreters::*;
use databend_query::sql::*;
use futures::TryStreamExt;

#[tokio::test]
async fn test_alter_recluster_interpreter() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    // Create table.
    {
        let query = "CREATE TABLE default.t(a bigint, b int) Engine = Fuse cluster by(a+1)";

        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    // insert into.
    {
        let query = "insert into default.t values(1,1),(3,3)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    // insert into.
    {
        let query = "insert into default.t values(2,2),(5,5)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    // insert into.
    {
        let query = "insert into default.t values(4,4)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    // cluster information.
    {
        let query = "select * from clustering_information('default', 't')";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = executor.execute(ctx.clone()).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
            "| cluster_by_keys | total_block_count | total_constant_block_count | average_overlaps | average_depth | block_depth_histogram |",
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
            "| ((a + 1))       | 3                 | 1                          | 1.3333           | 2             | {\"00002\":3}           |",
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // recluster.
    {
        let query = "ALTER TABLE default.t RECLUSTER FINAL where a != 4";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    // cluster information.
    {
        let query = "select * from clustering_information('default', 't')";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = executor.execute(ctx.clone()).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
            "| cluster_by_keys | total_block_count | total_constant_block_count | average_overlaps | average_depth | block_depth_histogram |",
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
            "| ((a + 1))       | 2                 | 1                          | 1                | 2             | {\"00002\":2}           |",
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // snapshot count.
    {
        let query = "select count(*) from fuse_snapshot('default', 't')";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = executor.execute(ctx.clone()).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+----------+",
            "| count(*) |",
            "+----------+",
            "| 4        |",
            "+----------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // clustering_history.
    {
        let query = "select count(*) from system.clustering_history";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = executor.execute(ctx.clone()).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+----------+",
            "| count(*) |",
            "+----------+",
            "| 1        |",
            "+----------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
