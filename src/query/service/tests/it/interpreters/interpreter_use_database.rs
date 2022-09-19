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
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn test_use_interpreter() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    let query = "USE default";
    let (plan, _, _) = planner.plan_sql(query).await?;
    let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
    assert_eq!(executor.name(), "UseDatabaseInterpreter");

    let mut stream = executor.execute(ctx.clone()).await?;
    while let Some(_block) = stream.next().await {}

    Ok(())
}

#[tokio::test]
async fn test_use_database_interpreter_error() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    let query = "USE xx";
    let (plan, _, _) = planner.plan_sql(query).await?;
    let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;

    if let Err(e) = executor.execute(ctx.clone()).await {
        let expect = "Code: 1003, displayText = Cannot USE 'xx', because the 'xx' doesn't exist.";
        assert_eq!(expect, e.to_string());
    }

    Ok(())
}
