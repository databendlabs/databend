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
use common_planners::*;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::interpreters::*;
use crate::sql::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_interpreter_interceptor() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::try_create_context()?;

    static TEST_QUERY: &str = "select number from numbers_mt(10)";
    if let PlanNode::Select(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
        let select_executor = SelectInterpreter::try_create(ctx.clone(), plan)?;
        let interceptor = InterceptorInterpreter::create(ctx, select_executor);
        assert_eq!(interceptor.name(), "SelectInterpreter");

        let stream = interceptor.execute(None).await?;
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
        interceptor.finish().await?;
    } else {
        panic!()
    }

    Ok(())
}
