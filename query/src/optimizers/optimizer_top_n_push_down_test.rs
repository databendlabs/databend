// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;

use crate::optimizers::*;

#[test]
fn test_simple() -> Result<()> {
    let query = "select number from numbers(1000) order by number limit 10;";
    let ctx = crate::tests::try_create_context()?;

    let plan = crate::tests::parse_query(query)?;

    let mut optimizer = TopNPushDownOptimizer::create(ctx);
    let plan_node = optimizer.optimize(&plan)?;

    let expect = "\
    Limit: 10\
    \n  Projection: number:UInt64\
    \n    Sort: number:UInt64\
    \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 1000, read_bytes: 8000], push_downs: [limit: 10, order_by: [number]]";

    let actual = format!("{:?}", plan_node);
    assert_eq!(expect, actual);
    Ok(())
}

#[test]
fn test_simple_with_offset() -> Result<()> {
    let query = "select number from numbers(1000) order by number limit 10 offset 5;";
    let ctx = crate::tests::try_create_context()?;

    let plan = crate::tests::parse_query(query)?;

    let mut optimizer = TopNPushDownOptimizer::create(ctx);
    let plan_node = optimizer.optimize(&plan)?;

    let expect = "\
    Limit: 10, 5\
    \n  Projection: number:UInt64\
    \n    Sort: number:UInt64\
    \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 1000, read_bytes: 8000], push_downs: [limit: 15, order_by: [number]]";

    let actual = format!("{:?}", plan_node);
    assert_eq!(expect, actual);
    Ok(())
}

#[test]
fn test_nested_projection() -> Result<()> {
    let query =
        "select number from (select * from numbers(1000) order by number limit 11) limit 10;";
    let ctx = crate::tests::try_create_context()?;

    let plan = crate::tests::parse_query(query)?;

    let mut optimizer = TopNPushDownOptimizer::create(ctx);
    let plan_node = optimizer.optimize(&plan)?;

    let expect = "\
    Limit: 10\
    \n  Projection: number:UInt64\
    \n    Limit: 11\
    \n      Projection: number:UInt64\
    \n        Sort: number:UInt64\
    \n          ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 1000, read_bytes: 8000], push_downs: [limit: 11, order_by: [number]]";

    let actual = format!("{:?}", plan_node);
    assert_eq!(expect, actual);
    Ok(())
}

#[test]
fn test_aggregate() -> Result<()> {
    let query =
        "select sum(number) FROM numbers(1000) group by number % 10 order by sum(number) limit 5;";
    let ctx = crate::tests::try_create_context()?;

    let plan = crate::tests::parse_query(query)?;

    let mut optimizer = TopNPushDownOptimizer::create(ctx);
    let plan_node = optimizer.optimize(&plan)?;

    let expect = "\
    Limit: 5\
    \n  Projection: sum(number):UInt64\
    \n    Sort: sum(number):UInt64\
    \n      AggregatorFinal: groupBy=[[(number % 10)]], aggr=[[sum(number)]]\
    \n        AggregatorPartial: groupBy=[[(number % 10)]], aggr=[[sum(number)]]\
    \n          Expression: (number % 10):UInt8, number:UInt64 (Before GroupBy)\
    \n            ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 1000, read_bytes: 8000], push_downs: []";

    let actual = format!("{:?}", plan_node);
    assert_eq!(expect, actual);
    Ok(())
}
