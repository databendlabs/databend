// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_filter_plan() -> crate::error::FuseQueryResult<()> {
    use crate::planners::*;
    use crate::testdata;
    use pretty_assertions::assert_eq;

    let test_source = testdata::NumberTestData::create();
    let source = test_source.number_read_source_plan_for_test(8)?;
    let plan = PlanBuilder::from(&PlanNode::ReadSource(source))
        .filter(field("number").eq(constant(1i64)))?
        .project(vec![field("number")])?
        .build()?;

    let expect = "\
    └─ Projection: number\
    \n  └─ Filter: number = 1\
    \n    └─ ReadDataSource: scan parts [4](Read from system.numbers table)";
    let actual = format!("{:?}", plan);

    assert_eq!(expect, actual);
    Ok(())
}
