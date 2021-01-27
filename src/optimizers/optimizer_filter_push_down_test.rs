// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_filter_push_down_optimizer() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::contexts::*;
    use crate::optimizers::*;
    use crate::planners::*;
    use crate::tests;

    let test_source = tests::NumberTestData::create();
    let ctx = FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;
    let plan = Planner::new().build_from_sql(
        ctx.clone(),
        "select (number+1) as c1, number as c2 from system.numbers_mt(10000) where (c1+c2+1)=1",
    )?;

    let mut filter_push_down = FilterPushDownOptimizer::create();
    let optimized = filter_push_down.optimize(&plan)?;
    let expect = "\
    Projection: (number + 1) as c1:UInt64, number as c2:UInt64\
    \n  Filter: ((((number + 1) + number) + 1) = 1)\
    \n    ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:10000, Read Bytes:80000)";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}
