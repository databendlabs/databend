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
async fn test_constant_folding_optimizer() -> Result<()> {
    struct Test {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests: Vec<Test> = vec![
            Test {
                name: "Projection const recursion",
                query: "SELECT 1 + 2 + 3",
                expect: "\
                Projection: ((1 + 2) + 3):UInt32\
                \n  Expression: 6:UInt32 (Before Projection)\
                \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
            Test {
                name: "Projection left non const recursion",
                query: "SELECT dummy + 1 + 2 + 3",
                expect: "\
                Projection: (((dummy + 1) + 2) + 3):UInt64\
                \n  Expression: (((dummy + 1) + 2) + 3):UInt64 (Before Projection)\
                \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
            Test {
                name: "Projection right non const recursion",
                query: "SELECT 1 + 2 + 3 + dummy",
                expect: "\
                Projection: (((1 + 2) + 3) + dummy):UInt64\
                \n  Expression: (6 + dummy):UInt64 (Before Projection)\
                \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
            Test {
                name: "Projection arithmetic const recursion",
                query: "SELECT 1 + 2 + 3 / 3",
                expect: "\
                Projection: ((1 + 2) + (3 / 3)):Float64\
                \n  Expression: 4:Float64 (Before Projection)\
                \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
            Test {
                name: "Projection comparisons const recursion",
                query: "SELECT 1 + 2 + 3 > 3",
                expect: "\
                Projection: (((1 + 2) + 3) > 3):Boolean\
                \n  Expression: true:Boolean (Before Projection)\
                \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
            Test {
                name: "Projection cast const recursion",
                query: "SELECT CAST(1 AS bigint)",
                expect: "\
                Projection: cast(1 as Int64):Int64\
                \n  Expression: 1:Int64 (Before Projection)\
                \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
            Test {
                name: "Projection hash const recursion",
                query: "SELECT sipHash('test_string')",
                expect: "\
                Projection: sipHash('test_string'):UInt64\
                \n  Expression: 11164312367746070837:UInt64 (Before Projection)\
                \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
            Test {
                name: "Projection strings const recursion",
                query: "SELECT SUBSTRING('1234567890' FROM 3 FOR 3)",
                expect: "\
                Projection: substring('1234567890', 3, 3):String\
                \n  Expression: 345:String (Before Projection)\
                \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
            Test {
                name: "Projection to type name const recursion",
                query: "SELECT toTypeName('1234567890')",
                expect: "\
                Projection: toTypeName('1234567890'):String\
                \n  Expression: String:String (Before Projection)\
                \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
            },
        ];

    for test in tests {
        let ctx = crate::tests::create_query_context()?;

        let plan = PlanParser::parse(ctx.clone(), test.query).await?;
        let mut optimizer = ConstantFoldingOptimizer::create(ctx);
        let optimized = optimizer.optimize(&plan)?;
        let actual = format!("{:?}", optimized);
        assert_eq!(test.expect, actual, "{:#?}", test.name);
    }
    Ok(())
}
