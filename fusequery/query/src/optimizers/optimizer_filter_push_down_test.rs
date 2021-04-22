// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_filter_push_down_optimizer() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::optimizers::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "select (number+1) as c1, number as c2 from numbers_mt(10000) where (c1+c2+1)=1"
    )?;

    let mut filter_push_down = FilterPushDownOptimizer::create(ctx);
    let optimized = filter_push_down.optimize(&plan)?;
    let expect = "\
    Projection: (number + 1) as c1:UInt64, number as c2:UInt64\
    \n  Filter: ((((number + 1) + number) + 1) = 1)\
    \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}
