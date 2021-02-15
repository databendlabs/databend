// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[test]
fn test_select_wildcard_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::datavalues::*;
    use crate::planners::*;

    let ctx = crate::sessions::FuseQueryContext::try_create()?;

    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "a",
        DataType::Utf8,
        false,
    )]));
    let plan = PlanBuilder::create(ctx, schema)
        .project(vec![field("a")])?
        .build()?;
    let select = PlanNode::Select(SelectPlan {
        input: Arc::new(plan),
    });
    let expect = "Projection: a:Utf8\n  ";
    let actual = format!("{:?}", select);
    assert_eq!(expect, actual);
    Ok(())
}
