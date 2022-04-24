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
use goldenfile::Mint;

use crate::interpreters::interpreter_goldenfiles;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_tables_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;

    // Setup.
    {
        // Create database.
        {
            let plan = PlanParser::parse(ctx.clone(), "create database db1").await?;
            let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None).await?;
        }

        // Use database.
        {
            let plan = PlanParser::parse(ctx.clone(), "use db1").await?;
            let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None).await?;
        }

        // Create table.
        {
            let plan = PlanParser::parse(ctx.clone(), "create table data(a Int)").await?;
            let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None).await?;
        }
        {
            let plan = PlanParser::parse(ctx.clone(), "create table bend(a Int)").await?;
            let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None).await?;
        }
    }

    // show tables.
    {
        let mut mint = Mint::new("tests/goldenfiles/data");
        let mut file = mint.new_goldenfile("show-tables.txt").unwrap();

        let cases = &[
            r#"show tables"#,
            r#"show full tables"#,
            r#"show tables like '%da%'"#,
            r#"show full tables like '%da%'"#,
            r#"show tables where table_name != 'data'"#,
            r#"show full tables where table_name != 'data'"#,
            r#"show tables from db1"#,
            r#"show full tables from db1"#,
            r#"show tables in db1"#,
            r#"show full tables in db1"#,
        ];

        for case in cases {
            interpreter_goldenfiles(&mut file, ctx.clone(), "ShowTablesInterpreter", case).await?;
        }
    }

    // Teardown.
    {
        let plan = PlanParser::parse(ctx.clone(), "drop database db1").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    Ok(())
}
