// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_explain_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::contexts::*;
    use crate::planners::*;
    use crate::tests;

    let test_source = tests::NumberTestData::create();
    let ctx = FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;
    let plan = Planner::new().build_from_sql(
        ctx.clone(),
        "select number as c1, number as c2, number as c3,(number+1) from system.numbers_mt where (number+1)=4",
    )?;

    let explain = PlanNode::Explain(ExplainPlan {
        plan: Box::new(plan),
    });
    let expect = "\
    └─ Projection: number as c1:UInt64, number as c2:UInt64, number as c3:UInt64, (number + 1):UInt64\
    \n  └─ Filter: ((number + 1) = 4)\
    \n    └─ ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:65536, Read Bytes:524288)";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    Ok(())
}
