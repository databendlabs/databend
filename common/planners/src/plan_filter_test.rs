// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_filter_plan() -> crate::error::PlannerResult<()> {
    use pretty_assertions::assert_eq;

    use crate::*;

    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .filter(col("number").eq(lit(1i64)))?
        .project(vec![col("number")])?
        .build()?;

    let expect = "\
    Projection: number:UInt64\
    \n  Filter: (number = 1)\
    \n    ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:10000, Read Bytes:80000)";
    let actual = format!("{:?}", plan);

    assert_eq!(expect, actual);
    Ok(())
}
