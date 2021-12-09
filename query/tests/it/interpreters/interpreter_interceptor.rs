// Copyright 2021 Datafuse Labs.
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

use common_base::tokio;
use common_exception::Result;
use databend_query::interpreters::*;
use databend_query::sql::*;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_interpreter_interceptor() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context()?;

    {
        let query = "select number from numbers_mt(10)";
        ctx.attach_query_str(query);
        let plan = PlanParser::parse(query, ctx.clone()).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;
        interpreter.start().await?;
        let stream = interpreter.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let block = &result[0];
        assert_eq!(block.num_columns(), 1);

        let expected = vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 0      |",
            "| 1      |",
            "| 2      |",
            "| 3      |",
            "| 4      |",
            "| 5      |",
            "| 6      |",
            "| 7      |",
            "| 8      |",
            "| 9      |",
            "+--------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
        interpreter.finish().await?;
    }

    // Check.
    {
        let query = "select log_type, read_rows, read_bytes, written_rows, written_bytes, result_rows, result_bytes, query_kind, query_text from system.query_log";
        let plan = PlanParser::parse(query, ctx.clone()).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;

        let stream = interpreter.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;

        let expected = vec![
            "+----------+-----------+------------+--------------+---------------+-------------+--------------+------------+-----------------------------------+",
            "| log_type | read_rows | read_bytes | written_rows | written_bytes | result_rows | result_bytes | query_kind | query_text                        |",
            "+----------+-----------+------------+--------------+---------------+-------------+--------------+------------+-----------------------------------+",
            "| 1        | 0         | 0          | 0            | 0             | 0           | 0            | SelectPlan | select number from numbers_mt(10) |",
            "| 2        | 10        | 80         | 0            | 0             | 10          | 80           | SelectPlan | select number from numbers_mt(10) |",
            "+----------+-----------+------------+--------------+---------------+-------------+--------------+------------+-----------------------------------+",
        ];

        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
