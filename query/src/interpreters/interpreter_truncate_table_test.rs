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

use common_base::tokio;
use common_exception::Result;
use common_planners::*;
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
            .build_from_sql("create table default.a(a String, b String) Engine = Memory")?
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
                "+-----+-----+",
                "| a   | b   |",
                "+-----+-----+",
                "| 1,1 | 2,2 |",
                "+-----+-----+",
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
