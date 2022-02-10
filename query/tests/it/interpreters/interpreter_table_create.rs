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
use futures::stream::StreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_table_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    {
        let query = "\
        CREATE TABLE default.a(\
            a bigint not null default 3, b int default a + 3, c varchar(255), d smallint, e Date\
        ) Engine = Null\
    ";

        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let mut stream = interpreter.execute(None).await?;
        while let Some(_block) = stream.next().await {}

        let schema = plan.schema();

        let field_a = schema.field_with_name("a").unwrap();
        assert_eq!(
            format!("{:?}", field_a),
            "DataField { name: \"a\", data_type: Int64, nullable: false, default_expr: \"{\\\"Literal\\\":{\\\"value\\\":{\\\"UInt64\\\":3},\\\"column_name\\\":null,\\\"data_type\\\":{\\\"type\\\":\\\"UInt8Type\\\"}}}\" }"
        );

        let field_b = schema.field_with_name("b").unwrap();
        assert_eq!(
            format!("{:?}", field_b),
           "DataField { name: \"b\", data_type: Int32, nullable: true, default_expr: \"{\\\"BinaryExpression\\\":{\\\"left\\\":{\\\"Column\\\":\\\"a\\\"},\\\"op\\\":\\\"+\\\",\\\"right\\\":{\\\"Literal\\\":{\\\"value\\\":{\\\"UInt64\\\":3},\\\"column_name\\\":null,\\\"data_type\\\":{\\\"type\\\":\\\"UInt8Type\\\"}}}}}\" }"
        );
    }

    {
        static TEST_CREATE_QUERY: &str = "\
            CREATE TABLE default.test_a(\
                a bigint, b int, c varchar(255), d smallint, e Date\
            ) Engine = Null\
        ";

        let plan = PlanParser::parse(ctx.clone(), TEST_CREATE_QUERY).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    {
        static TEST_CREATE_QUERY_SELECT: &str =
            "CREATE TABLE default.test_b(a varchar, x int) select b, a from default.test_a";

        let plan = PlanParser::parse(ctx.clone(), TEST_CREATE_QUERY_SELECT).await?;
        let interpreter = InterpreterFactory::get(ctx, plan.clone())?;
        let mut stream = interpreter.execute(None).await?;
        while let Some(_block) = stream.next().await {}

        let schema = plan.schema();

        let field_a = schema.field_with_name("a").unwrap();
        assert_eq!(
            format!("{:?}", field_a),
            r#"DataField { name: "a", data_type: String, nullable: true }"#
        );

        let field_x = schema.field_with_name("x").unwrap();
        assert_eq!(
            format!("{:?}", field_x),
            r#"DataField { name: "x", data_type: Int32, nullable: true }"#
        );

        let field_b = schema.field_with_name("b").unwrap();
        assert_eq!(
            format!("{:?}", field_b),
            r#"DataField { name: "b", data_type: Int32, nullable: true }"#
        );
    }

    Ok(())
}
