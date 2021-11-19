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

use crate::interpreters::*;
use crate::sql::*;

#[tokio::test]
async fn test_insert_into_select_interpreter() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    // Create input table.
    {
        if let PlanNode::CreateTable(plan) = PlanParser::create(ctx.clone())
            .build_from_sql("create table default.input_table(a String, b String, c String, d String, e String) Engine = Memory")?
        {
            let executor = CreateTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = executor.execute().await?;
        }
    }

    // Create output table.
    {
        if let PlanNode::CreateTable(plan) = PlanParser::create(ctx.clone())
            .build_from_sql("create table default.output_table(a UInt8, b Int8, c UInt16, d Int16, e String) Engine = Memory")?
        {
            let executor = CreateTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = executor.execute().await?;
        }
    }

    // Insert into input table.
    {
        if let PlanNode::InsertInto(plan) = PlanParser::create(ctx.clone())
            .build_from_sql("insert into default.input_table values(1,1,1,1,1), (2,2,2,2,2)")?
        {
            let executor = InsertIntoInterpreter::try_create(ctx.clone(), plan.clone(), None)?;
            let _ = executor.execute().await?;
        }
    }

    // Insert into output table.
    {
        let plan_node = PlanParser::create(ctx.clone())
            .build_from_sql("insert into default.output_table select * from default.input_table")?;
        {
            let executor = InterpreterFactory::get(ctx.clone(), plan_node)?;
            let _ = executor.execute().await?;
        }
    }

    // select.
    {
        if let PlanNode::Select(plan) =
            PlanParser::create(ctx.clone()).build_from_sql("select * from default.output_table")?
        {
            let executor = SelectInterpreter::try_create(ctx.clone(), plan.clone())?;
            let stream = executor.execute().await?;
            let result = stream.try_collect::<Vec<_>>().await?;
            let expected = vec![
                "+---+---+---+---+---+",
                "| a | b | c | d | e |",
                "+---+---+---+---+---+",
                "| 1 | 1 | 1 | 1 | 1 |",
                "| 2 | 2 | 2 | 2 | 2 |",
                "+---+---+---+---+---+",
            ];
            common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
        } else {
            panic!()
        }
    }

    Ok(())
}
