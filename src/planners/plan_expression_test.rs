// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_expression_plan() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use crate::planners::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let plan = PlanBuilder::from(
        ctx.clone(),
        &PlanNode::ReadSource(test_source.number_read_source_plan_for_test(10000)?),
    )
    .filter(
        add(col("number"), lit(1))
            .eq(lit(4))
            .and(col("number").not_eq(lit(4)))
            .and(col("number").lt(lit(4)))
            .and(col("number").lt_eq(lit(4)))
            .and(col("number").gt(lit(4)))
            .and(col("number").gt_eq(lit(4))),
    )?
    .build()?;
    let explain = PlanNode::Explain(ExplainPlan {
        typ: DFExplainType::Syntax,
        input: Arc::new(plan),
    });
    let expect ="Filter: (((((((number + 1) = 4) and (number != 4)) and (number < 4)) and (number <= 4)) and (number > 4)) and (number >= 4))\
    \n  ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:10000, Read Bytes:80000)";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    Ok(())
}
