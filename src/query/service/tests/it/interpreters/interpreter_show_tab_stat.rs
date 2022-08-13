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
use databend_query::sql::Planner;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_tab_stat_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    // Setup.
    {
        // Create database.
        {
            let query = "create database db1";
            let (plan, _, _) = planner.plan_sql(query).await?;
            let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
            let _ = executor.execute().await?;
        }

        // Use database.
        {
            let query = "use db1";
            let (plan, _, _) = planner.plan_sql(query).await?;
            let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
            let _ = executor.execute().await?;
        }

        // Create table.
        {
            let query = "create table data(a Int)";
            let (plan, _, _) = planner.plan_sql(query).await?;
            let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
            let _ = executor.execute().await?;
        }
        {
            let query = "create table bend(a Int)";
            let (plan, _, _) = planner.plan_sql(query).await?;
            let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
            let _ = executor.execute().await?;
        }
    }

    // show table status like '%da%'.
    {
        let query = "show table status like '%da%'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "SelectInterpreterV2");
        let stream = executor.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            r"\+------\+--------\+---------\+------------\+------\+----------------\+-------------\+-----------------\+--------------\+-----------\+----------------\+-------------------------------\+-------------\+------------\+-----------\+----------\+---------\+",
            r"\| name \| engine \| version \| row_format \| rows \| avg_row_length \| data_length \| max_data_length \| index_length \| data_free \| auto_increment \| create_time                   \| update_time \| check_time \| collation \| checksum \| comment \|",
            r"\+------\+--------\+---------\+------------\+------\+----------------\+-------------\+-----------------\+--------------\+-----------\+----------------\+-------------------------------\+-------------\+------------\+-----------\+----------\+---------\+",
            r"\| data \| FUSE   \| 0       \| NULL       \| 0    \| NULL           \| 0           \| NULL            \| NULL         \| NULL      \| NULL           \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL        \| NULL       \| NULL      \| NULL     \|         \|",
            r"\+------\+--------\+---------\+------------\+------\+----------------\+-------------\+-----------------\+--------------\+-----------\+----------------\+-------------------------------\+-------------\+------------\+-----------\+----------\+---------\+",
        ];
        common_datablocks::assert_blocks_sorted_eq_with_regex(expected, result.as_slice());
    }

    // show table status where Name != 'data'.
    {
        let query = "show table status where Name != 'data'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "SelectInterpreterV2");
        let stream = executor.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            r"\+------\+--------\+---------\+------------\+------\+----------------\+-------------\+-----------------\+--------------\+-----------\+----------------\+-------------------------------\+-------------\+------------\+-----------\+----------\+---------\+",
            r"\| name \| engine \| version \| row_format \| rows \| avg_row_length \| data_length \| max_data_length \| index_length \| data_free \| auto_increment \| create_time                   \| update_time \| check_time \| collation \| checksum \| comment \|",
            r"\+------\+--------\+---------\+------------\+------\+----------------\+-------------\+-----------------\+--------------\+-----------\+----------------\+-------------------------------\+-------------\+------------\+-----------\+----------\+---------\+",
            r"\| bend \| FUSE   \| 0       \| NULL       \| 0    \| NULL           \| 0           \| NULL            \| NULL         \| NULL      \| NULL           \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL        \| NULL       \| NULL      \| NULL     \|         \|",
            r"\+------\+--------\+---------\+------------\+------\+----------------\+-------------\+-----------------\+--------------\+-----------\+----------------\+-------------------------------\+-------------\+------------\+-----------\+----------\+---------\+",
        ];
        common_datablocks::assert_blocks_sorted_eq_with_regex(expected, result.as_slice());
    }

    // show table status from db1.
    {
        let query = "show table status from db1";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "SelectInterpreterV2");
        let stream = executor.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            r"\+------\+--------\+---------\+------------\+------\+----------------\+-------------\+-----------------\+--------------\+-----------\+----------------\+-------------------------------\+-------------\+------------\+-----------\+----------\+---------\+",
            r"\| name \| engine \| version \| row_format \| rows \| avg_row_length \| data_length \| max_data_length \| index_length \| data_free \| auto_increment \| create_time                   \| update_time \| check_time \| collation \| checksum \| comment \|",
            r"\+------\+--------\+---------\+------------\+------\+----------------\+-------------\+-----------------\+--------------\+-----------\+----------------\+-------------------------------\+-------------\+------------\+-----------\+----------\+---------\+",
            r"\| bend \| FUSE   \| 0       \| NULL       \| 0    \| NULL           \| 0           \| NULL            \| NULL         \| NULL      \| NULL           \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL        \| NULL       \| NULL      \| NULL     \|         \|",
            r"\| data \| FUSE   \| 0       \| NULL       \| 0    \| NULL           \| 0           \| NULL            \| NULL         \| NULL      \| NULL           \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL        \| NULL       \| NULL      \| NULL     \|         \|",
            r"\+------\+--------\+---------\+------------\+------\+----------------\+-------------\+-----------------\+--------------\+-----------\+----------------\+-------------------------------\+-------------\+------------\+-----------\+----------\+---------\+",
        ];
        common_datablocks::assert_blocks_sorted_eq_with_regex(expected, result.as_slice());
    }

    // Teardown.
    {
        let query = "drop database db1";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        let _ = executor.execute().await?;
    }

    Ok(())
}
