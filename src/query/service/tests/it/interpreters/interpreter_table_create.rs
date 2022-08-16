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

use common_base::base::tokio;
use common_exception::Result;
use databend_query::interpreters::*;
use databend_query::sql::Planner;
use futures::StreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_table_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    {
        static TEST_CREATE_QUERY: &str = "\
            CREATE TABLE default.test_a(\
                a bigint, b int, c varchar(255), d smallint, e Date\
            ) Engine = Null\
        ";

        let (plan, _, _) = planner.plan_sql(TEST_CREATE_QUERY).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        let _ = executor.execute().await?;
    }

    {
        static TEST_CREATE_QUERY_SELECT: &str =
            "CREATE TABLE default.test_b(a varchar, x int) as select b, a from default.test_a";

        let (plan, _, _) = planner.plan_sql(TEST_CREATE_QUERY_SELECT).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}

        let schema = plan.schema();

        let field_a = schema.field_with_name("a").unwrap();
        assert_eq!(
            format!("{:?}", field_a),
            r#"DataField { name: "a", data_type: String, nullable: false }"#
        );

        let field_x = schema.field_with_name("x").unwrap();
        assert_eq!(
            format!("{:?}", field_x),
            r#"DataField { name: "x", data_type: Int32, nullable: false }"#
        );

        let field_b = schema.field_with_name("b").unwrap();
        assert_eq!(
            format!("{:?}", field_b),
            r#"DataField { name: "b", data_type: Int32, nullable: false }"#
        );
    }

    // create table with column comment in the new planner.
    {
        let query = "
            CREATE TABLE t(\
            a bigint comment 'a', b int comment 'b',\
            c varchar(255) comment 'c', d smallint comment 'd', e Date comment 'e')\
            Engine = Null COMMENT = 'test create'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;

        assert!(executor.execute().await.is_ok());
    }

    Ok(())
}
