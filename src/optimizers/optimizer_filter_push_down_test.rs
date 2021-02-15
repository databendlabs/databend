// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[test]
fn test_filter_push_down_optimizer() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::optimizers::*;
    use crate::sql::*;

    let ctx = crate::sessions::FuseQueryContext::try_create_ctx()?;
    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "select (number+1) as c1, number as c2 from system.numbers_mt(10000) where (c1+c2+1)=1",
    )?;

    let mut filter_push_down = FilterPushDownOptimizer::create(ctx);
    let optimized = filter_push_down.optimize(&plan)?;
    let expect = "\
    Projection: (number + 1) as c1:UInt64, number as c2:UInt64\
    \n  Filter: ((((number + 1) + number) + 1) = 1)\
    \n    ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:10000, Read Bytes:80000)";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}
