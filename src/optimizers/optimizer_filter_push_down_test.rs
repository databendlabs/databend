// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_filter_push_down() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::contexts::*;
    use crate::optimizers::*;
    use crate::planners::*;
    use crate::testdata;

    let test_source = testdata::NumberTestData::create();
    let ctx = Arc::new(FuseQueryContext::create_ctx(
        0,
        test_source.number_source_for_test()?,
    ));
    let plan = Planner::new().build_from_sql(
        ctx.clone(),
        "select (number+1) as c1, number from system.numbers_mt where c1=1",
    )?;

    let mut filter_push_down = FilterPushDownOptimizer::create();
    let optimized = filter_push_down.optimize(&plan)?;
    let expect = "\
    └─ Projection: number + 1 as c1, number\
    \n  └─ Filter: number + 1 = 1\
    \n    └─ ReadDataSource: scan parts [8](Read from system.numbers_mt table)";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}
