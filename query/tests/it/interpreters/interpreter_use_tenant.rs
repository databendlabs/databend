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
use common_planners::*;
use databend_query::configs::Config;
use databend_query::interpreters::*;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

use crate::tests::parse_query;

#[tokio::test]
async fn test_use_tenant_interpreter() -> Result<()> {
    let mut config = Config::default();
    config.query.proxy_mode = true;
    let ctx = crate::tests::create_query_context_with_config(config.clone())?;

    if let PlanNode::UseTenant(plan) = parse_query("USE TENANT 't1'", &ctx)? {
        let interpreter = UseTenantInterpreter::try_create(ctx.clone(), plan)?;
        assert_eq!(interpreter.name(), "UseTenantInterpreter");

        let mut stream = interpreter.execute(None).await?;
        while let Some(_block) = stream.next().await {}

        assert_eq!(ctx.get_tenant().as_str(), "t1");
    } else {
        panic!()
    }

    Ok(())
}

#[tokio::test]
async fn test_use_tenant_interpreter_error() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    let plan = parse_query("USE TENANT 't1'", &ctx)?;
    let interpreter = InterpreterFactory::get(ctx, plan)?;

    if let Err(e) = interpreter.execute(None).await {
        let expect = "Code: 62, displayText = Access denied:'USE TENANT' only used in proxy-mode.";
        assert_eq!(expect, format!("{}", e));
    } else {
        panic!();
    }

    Ok(())
}
