// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_select_wildcard_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::datavalues::*;
    use crate::planners::*;

    let ctx = crate::tests::try_create_context()?;

    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "a",
        DataType::Utf8,
        false,
    )]));
    let plan = PlanBuilder::create(ctx, schema)
        .project(vec![col("a")])?
        .build()?;
    let select = PlanNode::Select(SelectPlan {
        input: Arc::new(plan),
    });
    let expect = "Projection: a:Utf8";
    let actual = format!("{:?}", select);
    assert_eq!(expect, actual);
    Ok(())
}
