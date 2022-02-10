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
async fn test_expression_transform_optimizer() -> Result<()> {
    struct Test {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests: Vec<Test> = vec![
            Test {
                name: "And expression",
                query: "select number from numbers_mt(10) where not(number>1 and number<=3)",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: ((number <= 1) or (number > 3))\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [(NOT ((number > 1) AND (number <= 3)))]]",
            },
            Test {
                name: "Complex expression",
                query: "select number from numbers_mt(10) where not(number>=5 or number<3 and toBoolean(number))",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: ((number < 5) and ((number >= 3) or (NOT toBoolean(number))))\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [(NOT ((number >= 5) OR ((number < 3) AND toBoolean(number))))]]",
            },
            Test {
                name: "Like and isNotNull expression",
                query: "select * from system.databases where not (isNotNull(name) and name LIKE '%sys%')",
                expect: "\
                Projection: name:String\
                \n  Filter: (isnull(name) or (name not like %sys%))\
                \n    ReadDataSource: scan schema: [name:String], statistics: [read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0], push_downs: [projections: [0], filters: [(NOT (isNotNull(name) AND (name LIKE %sys%)))]]",
            },
            Test {
                name: "Not like and isNull expression",
                query: "select * from system.databases where not (name is null or name not like 'a%')",
                expect: "\
                Projection: name:String\
                \n  Filter: (isnotnull(name) and (name like a%))\
                \n    ReadDataSource: scan schema: [name:String], statistics: [read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0], push_downs: [projections: [0], filters: [(NOT (isnull(name) OR (name NOT LIKE a%)))]]",
            },
            Test {
                name: "Equal expression",
                query: "select number from numbers_mt(10) where not(number=1) and number<5",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: ((number <> 1) and (number < 5))\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [((NOT (number = 1)) AND (number < 5))]]",
            },
            Test {
                name: "Not equal expression",
                query: "select number from numbers_mt(10) where not(number!=1) or number<5",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: ((number = 1) or (number < 5))\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [((NOT (number <> 1)) OR (number < 5))]]",
            },
            Test {
                name: "Not expression",
                query: "select number from numbers_mt(10) where not(NOT toBoolean(number))",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: toBoolean(number)\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [(NOT (NOT toBoolean(number)))]]",
            },
            Test {
                name: "Boolean transform",
                query: "select number from numbers_mt(10) where number",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number != 0)\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [number]]",
            },
            Test {
                name: "Boolean and truth transform",
                query: "select number from numbers_mt(10) where not number",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number = 0)\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [(NOT number)]]",
            },
            Test {
                name: "Literal boolean transform",
                query: "select number from numbers_mt(10) where false",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: false\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0], push_downs: [projections: [0], filters: [false]]",
            },
            Test {
                name: "Limit zero transform",
                query: "select number from numbers_mt(10) where true limit 0",
                expect: "\
                Limit: 0\
                \n  Projection: number:UInt64\
                \n    Filter: true\
                \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0], push_downs: [projections: [0], filters: [true]]",
            },
            Test {
                name: "Filter true and cond",
                query: "SELECT number from numbers(10) where true AND number > 1",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number > 1)\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [(true AND (number > 1))]]",
            },
            Test {
                name: "Filter cond and true",
                query: "SELECT number from numbers(10) where number > 1 AND true",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number > 1)\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [((number > 1) AND true)]]",
            },
            Test {
                name: "Filter false and cond",
                query: "SELECT number from numbers(10) where false AND number > 1",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: false\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [(false AND (number > 1))]]",
            },
            Test {
                name: "Filter cond and false",
                query: "SELECT number from numbers(10) where number > 1 AND false",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: false\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [((number > 1) AND false)]]",
            },
            Test {
                name: "Filter false or cond",
                query: "SELECT number from numbers(10) where false OR number > 1",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number > 1)\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [(false OR (number > 1))]]",
            },
            Test {
                name: "Filter cond or false",
                query: "SELECT number from numbers(10) where number > 1 OR false",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number > 1)\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [((number > 1) OR false)]]",
            },
            Test {
                name: "Filter true or cond",
                query: "SELECT number from numbers(10) where true OR number > 1",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: true\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [(true OR (number > 1))]]",
            },
            Test {
                name: "Filter cond or true",
                query: "SELECT number from numbers(10) where number > 1 OR true",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: true\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [((number > 1) OR true)]]",
            },
            Test {
                name: "Projection logics const",
                query: "SELECT 1 = 1 and 2 > 1",
                expect: "\
                Projection: ((1 = 1) and (2 > 1)):Boolean\
                \n  Expression: ((1 = 1) and (2 > 1)):Boolean (Before Projection)\
                \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
            Test {
                name: "Projection cond",
                query: "select number<3 or number>5 from numbers(10);",
                expect: "\
                Projection: ((number < 3) or (number > 5)):Boolean\
                \n  Expression: ((number < 3) or (number > 5)):Boolean (Before Projection)\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
        ];

    for test in tests {
        let ctx = crate::tests::create_query_context()?;

        let plan = PlanParser::parse(ctx.clone(), test.query).await?;
        let mut optimizer = ExprTransformOptimizer::create(ctx);
        let optimized = optimizer.optimize(&plan)?;
        let actual = format!("{:?}", optimized);
        assert_eq!(test.expect, actual, "{:#?}", test.name);
    }
    Ok(())
}
