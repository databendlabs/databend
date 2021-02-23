// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[test]
fn test_plan_walker() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::planners::*;

    let ctx = crate::sessions::FuseQueryContext::try_create()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let plan = PlanBuilder::from(
        ctx.clone(),
        &PlanNode::ReadSource(test_source.number_read_source_plan_for_test(10000)?),
    )
    .aggregate_partial(vec![sum(field("number")).alias("sumx")], vec![])?
    .aggregate_final(vec![sum(field("number")).alias("sumx")], vec![])?
    .project(vec![field("sumx")])?
    .build()?;

    // PreOrder.
    {
        let mut actual: Vec<String> = vec![];
        walk_preorder(plan.input().as_ref(), |plan| {
            actual.push(plan.name().to_string());
            return Ok(true);
        })?;

        let expect = vec![
            "AggregatorFinalPlan".to_string(),
            "AggregatorPartialPlan".to_string(),
            "ReadSourcePlan".to_string(),
        ];
        assert_eq!(expect, actual);
    }

    // PostOrder.
    {
        let mut actual: Vec<String> = vec![];
        walk_postorder(plan.input().as_ref(), |plan| {
            actual.push(plan.name().to_string());
            return Ok(true);
        })?;

        let expect = vec![
            "ReadSourcePlan".to_string(),
            "AggregatorPartialPlan".to_string(),
            "AggregatorFinalPlan".to_string(),
        ];
        assert_eq!(expect, actual);
    }

    Ok(())
}
