// Copyright 2020 Datafuse Labs.
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
use databend_query::interpreters::*;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

use crate::tests::parse_query;

#[tokio::test]
async fn test_use_interpreter() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::UseDatabase(plan) = parse_query("USE default", &ctx)? {
        let interpreter = UseDatabaseInterpreter::try_create(ctx, plan)?;
        assert_eq!(interpreter.name(), "UseDatabaseInterpreter");

        let mut stream = interpreter.execute(None).await?;
        while let Some(_block) = stream.next().await {}
    } else {
        panic!()
    }

    Ok(())
}

#[tokio::test]
async fn test_use_database_interpreter_error() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::UseDatabase(plan) = parse_query("USE xx", &ctx)? {
        let interpreter = UseDatabaseInterpreter::try_create(ctx, plan)?;

        if let Err(e) = interpreter.execute(None).await {
            let expect = "Code: 3, displayText = Cannot USE 'xx', because the 'xx' doesn't exist.";
            assert_eq!(expect, format!("{}", e));
        } else {
            panic!();
        }
    }

    Ok(())
}
