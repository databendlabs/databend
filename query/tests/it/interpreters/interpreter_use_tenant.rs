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
use pretty_assertions::assert_eq;

#[tokio::test]
async fn test_use_tenant_interpreter() -> Result<()> {
    let conf = crate::tests::ConfigBuilder::create()
        .with_management_mode()
        .config();
    let ctx = crate::tests::create_query_context_with_config(conf.clone())?;

    let plan = PlanParser::parse(ctx.clone(), "SUDO USE TENANT 't1'").await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;

    assert_eq!(interpreter.name(), "UseTenantInterpreter");

    let mut stream = interpreter.execute(None).await?;
    while let Some(_block) = stream.next().await {}

    assert_eq!(ctx.get_tenant().as_str(), "t1");

    Ok(())
}

#[tokio::test]
async fn test_use_tenant_interpreter_error() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    let plan = PlanParser::parse(ctx.clone(), "SUDO USE TENANT 't1'").await?;
    let interpreter = InterpreterFactory::get(ctx, plan)?;

    if let Err(e) = interpreter.execute(None).await {
        let expect =
            "Code: 1062, displayText = Access denied:'USE TENANT' only used in management-mode.";
        assert_eq!(expect, format!("{}", e));
    } else {
        panic!();
    }

    Ok(())
}
