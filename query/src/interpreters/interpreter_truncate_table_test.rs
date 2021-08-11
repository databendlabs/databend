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

#[tokio::test]
async fn test_truncate_table_interpreter() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    // Create table.
    {
        if let PlanNode::CreateTable(plan) = PlanParser::create(ctx.clone())
            .build_from_sql("create table default.a(a bigint, b int) Engine = Memory")?
        {
            let executor = CreateTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = executor.execute().await?;
        }
    }

    // Insert into.
    {
        if let PlanNode::InsertInto(plan) = PlanParser::create(ctx.clone())
            .build_from_sql("insert into default.a values('1,1', '2,2')")?
        {
            let executor = InsertIntoInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = executor.execute().await?;
        }
    }

    // select.
    {
        if let PlanNode::Select(plan) =
            PlanParser::create(ctx.clone()).build_from_sql("select * from default.a")?
        {
            let executor = SelectInterpreter::try_create(ctx.clone(), plan.clone())?;
            let stream = executor.execute().await?;
            let result = stream.try_collect::<Vec<_>>().await?;
            let expected = vec![
                "+-------+-------+",
                "| a     | b     |",
                "+-------+-------+",
                "| '1,1' | '2,2' |",
                "+-------+-------+",
            ];
            common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
        } else {
            assert!(false)
        }
    }

    // truncate table.
    {
        if let PlanNode::TruncateTable(plan) =
            PlanParser::create(ctx.clone()).build_from_sql("truncate table default.a")?
        {
            let executor = TruncateTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            assert_eq!(executor.name(), "TruncateTableInterpreter");

            let stream = executor.execute().await?;
            let result = stream.try_collect::<Vec<_>>().await?;
            let expected = vec!["++", "++"];
            common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
        } else {
            assert!(false)
        }
    }

    // select.
    {
        if let PlanNode::Select(plan) =
            PlanParser::create(ctx.clone()).build_from_sql("select * from default.a")?
        {
            let executor = SelectInterpreter::try_create(ctx.clone(), plan.clone())?;
            let stream = executor.execute().await?;
            let result = stream.try_collect::<Vec<_>>().await?;
            let expected = vec!["++", "++"];
            common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
        } else {
            assert!(false)
        }
    }

    Ok(())
}
