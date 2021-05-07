// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_filter_alias_push_down_optimizer() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::optimizers::*;
    use crate::sql::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        query: &'static str,
        expect: &'static str
    }

    let tests = vec![
        Test {
            name:"filter-alias-push-down",
            query: "select (number+1) as c1, number as c2 from numbers_mt(10000) where (c1+c2+1)=1",
            expect: "\
    Projection: (number + 1) as c1:UInt64, number as c2:UInt64\
    \n  Filter: ((((number + 1) + number) + 1) = 1)\
    \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]",
    },
        Test {
            name:"group-by-alias-push-down",
            query: "select (number+1) as c1, (number%3+1) as c2 from numbers_mt(10000) group by c2",
            expect: "\
            AggregatorFinal: groupBy=[[((number % 3) + 1) as c2]], aggr=[[(number + 1) as c1, ((number % 3) + 1) as c2]]\
            \n  RedistributeStage[state: AggregatorMerge, id: 0]\
            \n    AggregatorPartial: groupBy=[[((number % 3) + 1) as c2]], aggr=[[(number + 1) as c1, ((number % 3) + 1) as c2]]\
            \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]",
        },
        Test {
            name:"having-alias-push-down",
            query: "select (number+1) as c1, (number%3+1) as c2 from numbers_mt(10000) having c1 > 10",
            expect: "\
            Having: (c1 > 10)\
            \n  Projection: (number + 1) as c1:UInt64, ((number % 3) + 1) as c2:UInt64\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]",
        },
        Test {
            name:"order-by-alias-push-down-now-work",
            query: "select (number+1) as c1, (number%3+1) as c2 from numbers_mt(10) order by c2",
            expect: "\
            Sort: c2:UInt64\
            \n  Projection: (number + 1) as c1:UInt64, ((number % 3) + 1) as c2:UInt64\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80]",
        },
    ];

    for t in tests {
        let ctx = crate::tests::try_create_context()?;

        let plan = PlanParser::create(ctx.clone()).build_from_sql(t.query)?;

        let mut optimizer = AliasPushDownOptimizer::create(ctx);
        let optimized = optimizer.optimize(&plan)?;
        let actual = format!("{:?}", optimized);
        assert_eq!(t.expect, actual);
    }

    Ok(())
}
