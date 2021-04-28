// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_group_by_push_down_optimizer() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::optimizers::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "select avg(number+1) as c1, (number%3+1) as c2 from numbers_mt(10000) group by c2"
    )?;

    let mut group_push_down = GroupByPushDownOptimizer::create(ctx);
    let optimized = group_push_down.optimize(&plan)?;
    let expect = "\
    AggregatorFinal: groupBy=[[((number % 3) + 1) as c2]], aggr=[[avg([(number + 1)]) as c1, ((number % 3) + 1) as c2]]\
    \n  RedistributeStage[state: AggregatorMerge, id: 0]\
    \n    AggregatorPartial: groupBy=[[((number % 3) + 1) as c2]], aggr=[[avg([(number + 1)]) as c1, ((number % 3) + 1) as c2]]\
    \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}
