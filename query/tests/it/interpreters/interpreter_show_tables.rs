// Copyright 2022 Datafuse Labs.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_tables_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;

    // Setup.
    {
        // Create database.
        {
            let plan = PlanParser::parse(ctx.clone(), "create database db1").await?;
            let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None, None).await?;
        }

        // Use database.
        {
            let plan = PlanParser::parse(ctx.clone(), "use db1").await?;
            let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None, None).await?;
        }

        // Create table.
        {
            let plan = PlanParser::parse(ctx.clone(), "create table data(a Int)").await?;
            let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None, None).await?;
        }
        {
            let plan = PlanParser::parse(ctx.clone(), "create table bend(a Int)").await?;
            let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None, None).await?;
        }
    }

    // show tables.
    {
        let plan = PlanParser::parse(ctx.clone(), "show tables").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowTablesInterpreter");
        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+",
            "| Tables_in_db1 |",
            "+---------------+",
            "| bend          |",
            "| data          |",
            "+---------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // show full tables.
    {
        let plan = PlanParser::parse(ctx.clone(), "show full tables").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowTablesInterpreter");
        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+------------+",
            "| Tables_in_db1 | Table_type |",
            "+---------------+------------+",
            "| bend          | BASE TABLE |",
            "| data          | BASE TABLE |",
            "+---------------+------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // show tables like '%da%'.
    {
        let plan = PlanParser::parse(ctx.clone(), "show tables like '%da%'").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowTablesInterpreter");
        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+",
            "| Tables_in_db1 |",
            "+---------------+",
            "| data          |",
            "+---------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // show full tables like '%da%'.
    {
        let plan = PlanParser::parse(ctx.clone(), "show full tables like '%da%'").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowTablesInterpreter");
        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+------------+",
            "| Tables_in_db1 | Table_type |",
            "+---------------+------------+",
            "| data          | BASE TABLE |",
            "+---------------+------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // show tables != 'data'.
    {
        let plan = PlanParser::parse(ctx.clone(), "show tables where table_name != 'data'").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowTablesInterpreter");
        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+",
            "| Tables_in_db1 |",
            "+---------------+",
            "| bend          |",
            "+---------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // show full tables != 'data'.
    {
        let plan =
            PlanParser::parse(ctx.clone(), "show full tables where table_name != 'data'").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowTablesInterpreter");
        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+------------+",
            "| Tables_in_db1 | Table_type |",
            "+---------------+------------+",
            "| bend          | BASE TABLE |",
            "+---------------+------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // show tables from db1.
    {
        let plan = PlanParser::parse(ctx.clone(), "show tables from db1").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowTablesInterpreter");
        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+",
            "| Tables_in_db1 |",
            "+---------------+",
            "| bend          |",
            "| data          |",
            "+---------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // show full tables from db1.
    {
        let plan = PlanParser::parse(ctx.clone(), "show full tables from db1").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowTablesInterpreter");
        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+------------+",
            "| Tables_in_db1 | Table_type |",
            "+---------------+------------+",
            "| bend          | BASE TABLE |",
            "| data          | BASE TABLE |",
            "+---------------+------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // show tables in db1.
    {
        let plan = PlanParser::parse(ctx.clone(), "show tables in db1").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowTablesInterpreter");
        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;

        let expected = vec![
            "+---------------+",
            "| Tables_in_db1 |",
            "+---------------+",
            "| bend          |",
            "| data          |",
            "+---------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // show full tables in db1.
    {
        let plan = PlanParser::parse(ctx.clone(), "show full tables in db1").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowTablesInterpreter");
        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;

        let expected = vec![
            "+---------------+------------+",
            "| Tables_in_db1 | Table_type |",
            "+---------------+------------+",
            "| bend          | BASE TABLE |",
            "| data          | BASE TABLE |",
            "+---------------+------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // Teardown.
    {
        let plan = PlanParser::parse(ctx.clone(), "drop database db1").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None, None).await?;
    }

    Ok(())
}
