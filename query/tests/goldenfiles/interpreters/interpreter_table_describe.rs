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
use goldenfile::Mint;

use crate::interpreters::interpreter_goldenfiles;

#[tokio::test]
async fn interpreter_describe_table_test() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;

    // Create table.
    {
        let query = "\
            CREATE TABLE default.a(\
                a bigint, b int, c varchar(255), d smallint, e Date \
            ) Engine = Null\
        ";

        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = interpreter.execute(None).await?;
    }

    // describe table.
    {
        let mut mint = Mint::new("tests/goldenfiles/data");
        let mut file = mint.new_goldenfile("table-describe.txt").unwrap();

        // `show fields from ` is same as `describe` table.
        let cases = &[r#"DESCRIBE a"#, r#"show fields from a"#];

        for case in cases {
            interpreter_goldenfiles(&mut file, ctx.clone(), "DescribeTableInterpreter", case)
                .await?;
        }
    }

    Ok(())
}
