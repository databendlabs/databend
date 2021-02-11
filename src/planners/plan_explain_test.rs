// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_explain_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::planners::*;

    let ctx = crate::contexts::FuseQueryContext::try_create_ctx()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let plan = PlanBuilder::create(ctx, test_source.number_schema_for_test()?)
        .project(vec![
            field("number").alias("c1"),
            field("number").alias("c2"),
        ])?
        .filter(add(field("number"), constant(1)).eq(constant(4)))?
        .build()?;
    let explain = PlanNode::Explain(ExplainPlan {
        typ: DFExplainType::Syntax,
        plan: Box::new(plan),
    });
    let expect = "Filter: ((number + 1) = 4)\
    \n  Projection: number as c1:UInt64, number as c2:UInt64\n    ";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    Ok(())
}
