// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_base::tokio;
use common_exception::Result;
use databend_query::optimizers::*;
use databend_query::sql::PlanParser;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple() -> Result<()> {
    let query = "select number from numbers(1000) order by number limit 10;";
    let ctx = crate::tests::create_query_context()?;

    let plan = PlanParser::parse(ctx.clone(), query).await?;

    let mut optimizer = TopNPushDownOptimizer::create(ctx);
    let plan_node = optimizer.optimize(&plan)?;

    let expect = "\
    Limit: 10\
    \n  Projection: number:UInt64\
    \n    Sort: number:UInt64\
    \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1000, read_bytes: 8000, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], limit: 10, order_by: [number]]";

    let actual = format!("{:?}", plan_node);
    assert_eq!(expect, actual);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_with_offset() -> Result<()> {
    let query = "select number from numbers(1000) order by number limit 10 offset 5;";
    let ctx = crate::tests::create_query_context()?;

    let plan = PlanParser::parse(ctx.clone(), query).await?;

    let mut optimizer = TopNPushDownOptimizer::create(ctx);
    let plan_node = optimizer.optimize(&plan)?;

    let expect = "\
    Limit: 10, 5\
    \n  Projection: number:UInt64\
    \n    Sort: number:UInt64\
    \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1000, read_bytes: 8000, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], limit: 15, order_by: [number]]";

    let actual = format!("{:?}", plan_node);
    assert_eq!(expect, actual);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_nested_projection() -> Result<()> {
    let query =
        "select number from (select * from numbers(1000) order by number limit 11) limit 10;";
    let ctx = crate::tests::create_query_context()?;

    let plan = PlanParser::parse(ctx.clone(), query).await?;

    let mut optimizer = TopNPushDownOptimizer::create(ctx);
    let plan_node = optimizer.optimize(&plan)?;

    let expect = "\
    Limit: 10\
    \n  Projection: number:UInt64\
    \n    Limit: 11\
    \n      Projection: number:UInt64\
    \n        Sort: number:UInt64\
    \n          ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1000, read_bytes: 8000, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], limit: 11, order_by: [number]]";

    let actual = format!("{:?}", plan_node);
    assert_eq!(expect, actual);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_aggregate() -> Result<()> {
    let query =
        "select sum(number) FROM numbers(1000) group by number % 10 order by sum(number) limit 5;";
    let ctx = crate::tests::create_query_context()?;

    let plan = PlanParser::parse(ctx.clone(), query).await?;

    let mut optimizer = TopNPushDownOptimizer::create(ctx);
    let plan_node = optimizer.optimize(&plan)?;

    let expect = "\
    Limit: 5\
    \n  Projection: sum(number):UInt64\
    \n    Sort: sum(number):UInt64\
    \n      AggregatorFinal: groupBy=[[(number % 10)]], aggr=[[sum(number)]]\
    \n        AggregatorPartial: groupBy=[[(number % 10)]], aggr=[[sum(number)]]\
    \n          Expression: (number % 10):UInt8, number:UInt64 (Before GroupBy)\
    \n            ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1000, read_bytes: 8000, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]";

    let actual = format!("{:?}", plan_node);
    assert_eq!(expect, actual);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_monotonic_function() -> Result<()> {
    struct Test {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "plus-only-function-(number+1)",
            query: "select number*number from numbers_mt(100) order by number+(number+ 3)",
            expect: "\
            Projection: (number * number):UInt64\
            \n  Sort: (number + (number + 3)):UInt64\
            \n    Expression: (number * number):UInt64, (number + (number + 3)):UInt64 (Before OrderBy)\
            \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100, read_bytes: 800, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
        },
        // TODO: broken this by select statement analyzer.
        Test {
            name: "plus-only-function-(number+number+3)",
            query: "select number*number from numbers_mt(100) order by number+number+3 limit 10",
            expect: "\
            Limit: 10\
            \n  Projection: (number * number):UInt64\
            \n    Sort: ((number + number) + 3):UInt64\
            \n      Expression: (number * number):UInt64, ((number + number) + 3):UInt64 (Before OrderBy)\
            \n        ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100, read_bytes: 800, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], limit: 10, order_by: [((number + number) + 3)]]",
        },
        //TODO: add more function tests
    ];

    for test in tests {
        let ctx = crate::tests::create_query_context()?;
        let plan = PlanParser::parse(ctx.clone(), test.query).await?;
        let mut optimizer = Optimizers::without_scatters(ctx);

        let optimized_plan = optimizer.optimize(&plan)?;
        let actual = format!("{:?}", optimized_plan);
        assert_eq!(test.expect, actual, "{:#?}", test.name);
    }
    Ok(())
}
