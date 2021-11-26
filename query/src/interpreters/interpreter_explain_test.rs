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
use crate::tests::parse_query;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_explain_interpreter() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    static TEST_QUERY: &str = "\
        EXPLAIN SELECT number FROM numbers_mt(10) \
        WHERE (number + 1) = 4 HAVING (number + 1) = 4\
    ";

    if let PlanNode::Explain(plan) = parse_query(TEST_QUERY, &ctx)? {
        let executor = ExplainInterpreter::try_create(ctx, plan)?;
        assert_eq!(executor.name(), "ExplainInterpreter");

        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let block = &result[0];
        assert_eq!(block.num_columns(), 1);
        assert_eq!(block.column(0).len(), 4);

        let expected = vec![
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| explain                                                                                                                                               |",
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| Projection: number:UInt64                                                                                                                             |",
            "|   Having: ((number + 1) = 4)                                                                                                                          |",
            "|     Filter: ((number + 1) = 4)                                                                                                                        |",
            "|       ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: [projections: [0]] |",
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];
        common_datablocks::assert_blocks_eq(expected, result.as_slice());
    } else {
        panic!()
    }

    Ok(())
}
