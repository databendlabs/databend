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
use common_users::UserApiProvider;
use databend_query::interpreters::*;
use databend_query::sessions::TableContext;
use databend_query::sql::*;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_udf_interpreter() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());
    let tenant = ctx.get_tenant();

    static CREATE_UDF: &str = "CREATE FUNCTION IF NOT EXISTS isnotempty AS (p) -> not(is_null(p)) DESC = 'This is a description'";

    static DROP_UDF_IF_EXISTS: &str = "DROP FUNCTION IF EXISTS isnotempty";
    static DROP_UDF: &str = "DROP FUNCTION isnotempty";

    {
        let (plan, _, _) = planner.plan_sql(CREATE_UDF).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        let mut stream = executor.execute(ctx.clone()).await?;
        while let Some(_block) = stream.next().await {}
        let udf = UserApiProvider::instance()
            .get_udf(&tenant, "isnotempty")
            .await?;

        assert_eq!(udf.name, "isnotempty");
        assert_eq!(udf.parameters, vec!["p".to_string()]);
        assert_eq!(udf.definition, "NOT is_null(p)");
        assert_eq!(udf.description, "This is a description")
    }

    {
        let (plan, _, _) = planner.plan_sql(DROP_UDF).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "DropUserUDFInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert!(res.is_ok());
    }

    {
        let (plan, _, _) = planner.plan_sql(DROP_UDF_IF_EXISTS).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "DropUserUDFInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert!(res.is_ok());
    }

    {
        let (plan, _, _) = planner.plan_sql(DROP_UDF).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "DropUserUDFInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert!(res.is_err());
    }

    {
        let (plan, _, _) = planner.plan_sql(CREATE_UDF).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "CreateUserUDFInterpreter");
        let mut stream = executor.execute(ctx.clone()).await?;
        while let Some(_block) = stream.next().await {}
        let udf = UserApiProvider::instance()
            .get_udf(&tenant, "isnotempty")
            .await?;

        assert_eq!(udf.name, "isnotempty");
        assert_eq!(udf.parameters, vec!["p".to_string()]);
        assert_eq!(udf.definition, "NOT is_null(p)");
        assert_eq!(udf.description, "This is a description")
    }

    {
        let (plan, _, _) = planner.plan_sql(DROP_UDF_IF_EXISTS).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "DropUserUDFInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert!(res.is_ok());
    }

    Ok(())
}
