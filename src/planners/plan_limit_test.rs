// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_limit_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::planners::*;

    let test_source = crate::tests::NumberTestData::create();
    let ctx =
        crate::contexts::FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;

    let limit = PlanNode::Limit(LimitPlan {
        n: 33,
        input: Arc::from(PlanBuilder::empty(ctx).build()?),
    });
    let expect = "Limit: 33\n  ";
    let actual = format!("{:?}", limit);
    assert_eq!(expect, actual);
    Ok(())
}
