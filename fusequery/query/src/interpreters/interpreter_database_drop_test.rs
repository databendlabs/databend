// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test]
async fn test_drop_database_interpreter() -> anyhow::Result<()> {
    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::interpreters::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::DropDatabase(plan) =
        PlanParser::create(ctx.clone()).build_from_sql("drop database default")?
    {
        let executor = DropDatabaseInterpreter::try_create(ctx, plan.clone())?;
        assert_eq!(executor.name(), "DropDatabaseInterpreter");
        let stream = executor.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec!["++", "++"];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    } else {
        assert!(false)
    }

    Ok(())
}
