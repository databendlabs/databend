// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

use crate::interpreters::*;
use crate::sql::*;

#[tokio::test]
async fn test_use_interpreter() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::UseDatabase(plan) =
        PlanParser::create(ctx.clone()).build_from_sql("use default")?
    {
        let executor = UseDatabaseInterpreter::try_create(ctx, plan)?;
        assert_eq!(executor.name(), "UseDatabaseInterpreter");

        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    Ok(())
}

#[tokio::test]
async fn test_use_database_interpreter_error() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::UseDatabase(plan) = PlanParser::create(ctx.clone()).build_from_sql("use xx")? {
        let executor = UseDatabaseInterpreter::try_create(ctx, plan)?;

        if let Err(e) = executor.execute().await {
            let expect = "Code: 3, displayText = Database xx  doesn\'t exist..";
            assert_eq!(expect, format!("{}", e));
        } else {
            assert!(false);
        }
    }

    Ok(())
}
