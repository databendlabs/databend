// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_filter_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::contexts::*;
    use crate::planners::*;
    use crate::tests;

    let test_source = tests::NumberTestData::create();
    let ctx = FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;
    let source = test_source.number_read_source_plan_for_test(ctx, 8)?;
    let plan = PlanBuilder::from(&PlanNode::ReadSource(source))
        .filter(field("number").eq(constant(1i64)))?
        .project(vec![field("number")])?
        .build()?;

    let expect = "\
    Projection: number:UInt64\
    \n  Filter: (number = 1)\
    \n    ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:8, Read Bytes:64)";
    let actual = format!("{:?}", plan);

    assert_eq!(expect, actual);
    Ok(())
}
