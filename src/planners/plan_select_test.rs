// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_select_wildcard_plan() -> crate::error::FuseQueryResult<()> {
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
        "select * from system.numbers_mt where (number+1)=4",
    )?;
    let expect = "\
    └─ Projection: number\
    \n  └─ Filter: ((number + 1) = 4)\
    \n    └─ ReadDataSource: scan parts [8](Read from system.numbers_mt table)";
    let actual = format!("{:?}", plan);
    assert_eq!(expect, actual);
    Ok(())
}
