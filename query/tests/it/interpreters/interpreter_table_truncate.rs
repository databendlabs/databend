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
use databend_query::sql::PlanParser;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn test_truncate_table_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    // Create table.
    {
        static TEST_CREATE_QUERY: &str = "\
            CREATE TABLE default.a(\
                a String, b String\
            ) Engine = Memory\
        ";

        let plan = PlanParser::parse(TEST_CREATE_QUERY, ctx.clone()).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = interpreter.execute(None).await?;
    }

    // Insert into.
    {
        static TEST_INSERT_QUERY: &str = "INSERT INTO default.a VALUES('1,1', '2,2')";
        let plan = PlanParser::parse(TEST_INSERT_QUERY, ctx.clone()).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    // select.
    {
        static TEST_SELECT_QUERY: &str = "SELECT * FROM default.a";
        let plan = PlanParser::parse(TEST_SELECT_QUERY, ctx.clone()).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
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
    }

    // truncate table.
    {
        static TEST_TRUNCATE_QUERY: &str = "TRUNCATE TABLE default.a";
        let plan = PlanParser::parse(TEST_TRUNCATE_QUERY, ctx.clone()).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(interpreter.name(), "TruncateTableInterpreter");

        let stream = interpreter.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec!["++", "++"];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // select.
    {
        static TEST_SELECT_QUERY: &str = "SELECT * FROM default.a";
        let plan = PlanParser::parse(TEST_SELECT_QUERY, ctx.clone()).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = interpreter.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec!["++", "++"];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
