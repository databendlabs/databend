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
use crate::tests::parse_query;

#[tokio::test]
async fn test_truncate_table_interpreter() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    // Create table.
    {
        static TEST_CREATE_QUERY: &str = "\
            CREATE TABLE default.a(\
                a String, b String\
            ) Engine = Memory\
        ";

        if let PlanNode::CreateTable(plan) = parse_query(TEST_CREATE_QUERY, &ctx)? {
            let interpreter = CreateTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = interpreter.execute(None).await?;
        }
    }

    // Insert into.
    {
        static TEST_INSERT_QUERY: &str = "INSERT INTO default.a VALUES('1,1', '2,2')";
        if let PlanNode::InsertInto(plan) = parse_query(TEST_INSERT_QUERY, &ctx)? {
            let executor = InsertIntoInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None).await?;
        }
    }

    // select.
    {
        static TEST_SELECT_QUERY: &str = "SELECT * FROM default.a";
        if let PlanNode::Select(plan) = parse_query(TEST_SELECT_QUERY, &ctx)? {
            let interpreter = SelectInterpreter::try_create(ctx.clone(), plan.clone())?;
            let stream = interpreter.execute(None).await?;
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
            panic!()
        }
    }

    // truncate table.
    {
        static TEST_TRUNCATE_QUERY: &str = "TRUNCATE TABLE default.a";
        if let PlanNode::TruncateTable(plan) = parse_query(TEST_TRUNCATE_QUERY, &ctx)? {
            let interpreter = TruncateTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            assert_eq!(interpreter.name(), "TruncateTableInterpreter");

            let stream = interpreter.execute(None).await?;
            let result = stream.try_collect::<Vec<_>>().await?;
            let expected = vec!["++", "++"];
            common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
        } else {
            panic!()
        }
    }

    // select.
    {
        static TEST_SELECT_QUERY: &str = "SELECT * FROM default.a";
        if let PlanNode::Select(plan) = parse_query(TEST_SELECT_QUERY, &ctx)? {
            let executor = SelectInterpreter::try_create(ctx.clone(), plan.clone())?;
            let stream = executor.execute(None).await?;
            let result = stream.try_collect::<Vec<_>>().await?;
            let expected = vec!["++", "++"];
            common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
        } else {
            panic!()
        }
    }

    Ok(())
}
