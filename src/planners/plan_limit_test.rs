// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_limit_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::planners::*;

    let limit = PlanNode::Limit(LimitPlan {
        n: 33,
        input: Arc::from(PlanBuilder::empty().build()?),
    });
    let expect = "Limit: 33\n  ";
    let actual = format!("{:?}", limit);
    assert_eq!(expect, actual);
    Ok(())
}
