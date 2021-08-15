// Copyright 2020 Datafuse Labs.
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

// Disabled until https://github.com/datafuselabs/datafuse/pull/550 finished
/*
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_select_interpreter() -> anyhow::Result<()> {
    use common_planners::*;
    use futures::TryStreamExt;

    use crate::interpreters::*;
    use crate::sql::*;

    let ctx =
        crate::tests::try_create_context()?.with_id("cf6db5fe-7595-4d85-97ee-71f051b21cbe")?;

    if let PlanNode::Select(plan) = PlanParser::create(ctx.clone())
        .build_from_sql("select number from numbers_mt(10) where (number+2)<2")?
    {
        let executor = SelectInterpreter::try_create(ctx.clone(), plan)?;
        assert_eq!(executor.name(), "SelectInterpreter");

        let stream = executor.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let block = &result[0];
        assert_eq!(block.num_columns(), 1);

        let expected = vec!["++", "||", "++", "++"];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    } else {
        assert!(false)
    }

    if let PlanNode::Select(plan) =
        PlanParser::create(ctx.clone()).build_from_sql("select 1 + 1, 2 + 2, 3 * 3, 4 * 4")?
    {
        let executor = SelectInterpreter::try_create(ctx.clone(), plan)?;
        assert_eq!(executor.name(), "SelectInterpreter");

        let stream = executor.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let block = &result[0];
        assert_eq!(block.num_columns(), 4);

        let expected = vec![
            "+------------+------------+----------------+----------------+",
            "| plus(1, 1) | plus(2, 2) | multiply(3, 3) | multiply(4, 4) |",
            "+------------+------------+----------------+----------------+",
            "| 2          | 4          | 9              | 16             |",
            "+------------+------------+----------------+----------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
*/
