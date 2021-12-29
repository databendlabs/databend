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
use databend_query::interpreters::*;
use databend_query::sql::*;
use futures::TryStreamExt;

#[tokio::test]
async fn test_insert_into_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    // Create default value table.
    {
        static TEST_QUERY: &str = "create table default.default_value_table(a String, b String DEFAULT 'b') Engine = Memory";
        if let PlanNode::CreateTable(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
            let executor = CreateTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None).await?;
        }
    }

    // Create input table.
    {
        static TEST_QUERY: &str = "create table default.input_table(a String, b String, c String, d String, e String) Engine = Memory";
        if let PlanNode::CreateTable(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
            let executor = CreateTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None).await?;
        }
    }

    // Create output table.
    {
        static TEST_QUERY: &str = "create table default.output_table(a UInt8, b Int8, c UInt16, d Int16, e String) Engine = Memory";
        if let PlanNode::CreateTable(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
            let executor = CreateTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None).await?;
        }
    }

    // Insert into default value table.
    {
        // insert into.
        {
            static TEST_QUERY: &str = "insert into default.default_value_table(a) values('a')";
            if let PlanNode::Insert(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
                let executor = InsertInterpreter::try_create(ctx.clone(), plan.clone())?;
                let _ = executor.execute(None).await?;
            }
        }

        // insert into select.
        {
            static TEST_QUERY: &str = "insert into default.default_value_table(a) select a from default.default_value_table";
            if let PlanNode::Insert(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
                let executor = InsertInterpreter::try_create(ctx.clone(), plan.clone())?;
                let _ = executor.execute(None).await?;
            }
        }

        // select.
        {
            static TEST_QUERY: &str = "select * from default.default_value_table";
            if let PlanNode::Select(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
                let executor = SelectInterpreter::try_create(ctx.clone(), plan.clone())?;
                let stream = executor.execute(None).await?;
                let result = stream.try_collect::<Vec<_>>().await?;
                let expected = vec![
                    "+---+---+",
                    "| a | b |",
                    "+---+---+",
                    "| a | b |",
                    "| a | b |",
                    "+---+---+",
                ];
                common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
            } else {
                panic!()
            }
        }
    }

    // Insert into input table.
    {
        static TEST_QUERY: &str = "insert into default.input_table values(1,1,1,1,1), (2,2,2,2,2)";
        if let PlanNode::Insert(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
            let executor = InsertInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None).await?;
        }
    }

    // Insert into output table.
    {
        static TEST_QUERY: &str =
            "insert into default.output_table select * from default.input_table";
        let plan_node = PlanParser::parse(TEST_QUERY, ctx.clone()).await?;
        {
            let executor = InterpreterFactory::get(ctx.clone(), plan_node)?;
            let _ = executor.execute(None).await?;
        }
    }

    // select.
    {
        static TEST_QUERY: &str = "select * from default.output_table";
        if let PlanNode::Select(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
            let executor = SelectInterpreter::try_create(ctx.clone(), plan.clone())?;
            let stream = executor.execute(None).await?;
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
