// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::interpreters::*;
use crate::sql::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_explain_interpreter() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::Explain(plan) = PlanParser::create(ctx.clone()).build_from_sql(
        "explain select number from numbers_mt(10) where (number+1)=4 having (number+1)=4",
    )? {
        let executor = ExplainInterpreter::try_create(ctx, plan)?;
        assert_eq!(executor.name(), "ExplainInterpreter");

        let stream = executor.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let block = &result[0];
        assert_eq!(block.num_columns(), 1);
        assert_eq!(block.column(0).len(), 4);

        let expected = vec![
            "+-----------------------------------------------------------------------------------------------------------------------+",
            "| explain                                                                                                               |",
            "+-----------------------------------------------------------------------------------------------------------------------+",
            "| Projection: number:UInt64                                                                                             |",
            "|   Having: ((number + 1) = 4)                                                                                          |",
            "|     Filter: ((number + 1) = 4)                                                                                        |",
            "|       ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80] |",
            "+-----------------------------------------------------------------------------------------------------------------------+",
        ];
        common_datablocks::assert_blocks_eq(expected, result.as_slice());
    } else {
        assert!(false)
    }

    Ok(())
}
