// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_explain_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::contexts::*;
    use crate::planners::*;
    use crate::testdata;

    let test_source = testdata::NumberTestData::create();
    let ctx = Arc::new(FuseQueryContext::create_ctx(
        0,
        test_source.number_source_for_test()?,
    ));
    let plan = Planner::new().build_from_sql(
        ctx.clone(),
        "explain select number as c1, number as c2, number as c3,(number+1) from system.numbers where (number+1)=4",
    )?;
    let expect = "└─ Projection: number as c1, number as c2, number as c3, number + 1\
    \n  └─ Filter: number + 1 = 4\
    \n    └─ ReadDataSource: scan parts [4](Read from system.numbers table)";
    let actual = format!("{:?}", plan);
    assert_eq!(expect, actual);
    Ok(())
}
