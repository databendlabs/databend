// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_filter_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::planners::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let source = test_source.number_read_source_plan_for_test(8)?;
    let plan = PlanBuilder::from(ctx, &PlanNode::ReadSource(source))
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
