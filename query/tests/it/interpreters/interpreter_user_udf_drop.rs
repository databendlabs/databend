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
use databend_query::sql::*;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_udf_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context()?;
    let tenant = ctx.get_tenant();

    static CREATE_UDF: &str =
        "CREATE FUNCTION IF NOT EXISTS isnotempty AS (p) -> not(isnull(p)) DESC = 'This is a description'";

    static DROP_UDF_IF_EXISTS: &str = "DROP FUNCTION IF EXISTS isnotempty";
    static DROP_UDF: &str = "DROP FUNCTION isnotempty";

    {
        let plan = PlanParser::parse(ctx.clone(), CREATE_UDF).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let mut stream = executor.execute(None).await?;
        while let Some(_block) = stream.next().await {}
        let udf = ctx
            .get_user_manager()
            .get_udf(&tenant, "isnotempty")
            .await?;

        assert_eq!(udf.name, "isnotempty");
        assert_eq!(udf.parameters, vec!["p".to_string()]);
        assert_eq!(udf.definition, "not(isnull(p))");
        assert_eq!(udf.description, "This is a description")
    }

    {
        let plan = PlanParser::parse(ctx.clone(), DROP_UDF).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "DropUserUDFInterpreter");
        let res = executor.execute(None).await;
        assert!(res.is_ok());
    }

    {
        let plan = PlanParser::parse(ctx.clone(), DROP_UDF_IF_EXISTS).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "DropUserUDFInterpreter");
        let res = executor.execute(None).await;
        assert!(res.is_ok());
    }

    {
        let plan = PlanParser::parse(ctx.clone(), DROP_UDF).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "DropUserUDFInterpreter");
        let res = executor.execute(None).await;
        assert!(res.is_err());
    }

    {
        let plan = PlanParser::parse(ctx.clone(), CREATE_UDF).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreateUserUDFInterpreter");
        let mut stream = executor.execute(None).await?;
        while let Some(_block) = stream.next().await {}
        let udf = ctx
            .get_user_manager()
            .get_udf(&tenant, "isnotempty")
            .await?;

        assert_eq!(udf.name, "isnotempty");
        assert_eq!(udf.parameters, vec!["p".to_string()]);
        assert_eq!(udf.definition, "not(isnull(p))");
        assert_eq!(udf.description, "This is a description")
    }
    {
        let plan = PlanParser::parse(ctx.clone(), DROP_UDF_IF_EXISTS).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "DropUserUDFInterpreter");
        let res = executor.execute(None).await;
        assert!(res.is_ok());
    }

    Ok(())
}
