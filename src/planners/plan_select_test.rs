// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_select_wildcard_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::datavalues::*;
    use crate::planners::*;

    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "a",
        DataType::Utf8,
        false,
    )]));
    let plan = PlanBuilder::create(schema)
        .project(vec![field("a")])?
        .build()?;
    let select = PlanNode::Select(SelectPlan {
        plan: Box::new(plan),
    });
    let expect = "Projection: a:Utf8\n  ";
    let actual = format!("{:?}", select);
    assert_eq!(expect, actual);
    Ok(())
}
