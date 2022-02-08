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

use common_exception::Result;
use common_planners::*;

use crate::test::Test;

#[test]
fn test_plan_builds() -> Result<()> {
    use pretty_assertions::assert_eq;

    struct TestCase {
        name: &'static str,
        plan: Result<PlanNode>,
        expect: &'static str,
        err: &'static str,
    }

    let source = Test::create().generate_source_plan_for_test(10000)?;
    let tests = vec![
        TestCase {
            name: "field(*)-pass",
            plan: (PlanBuilder::from(&source)
                .expression(&[Expression::Wildcard], "")?
                .project(&[col("number")])?
                .build()),
            expect: "\
            Projection: number:UInt64\
            \n  Expression: number:UInt64 ()\
            \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000, partitions_scanned: 8, partitions_total: 8]",
            err : "",
        },
        TestCase {
            name: "constant-alias-pass",
            plan: (PlanBuilder::from(&source)
                .expression(&[add(lit(4), lit(5)), add(add(lit(4), lit(5)), lit(2))], "")?
                .project(&[add(lit(4), lit(5)).alias("4_5")])?
                .build()),
            expect: "\
            Projection: (4 + 5) as 4_5:Int16\
            \n  Expression: (4 + 5):Int16, ((4 + 5) + 2):Int32 ()\
            \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000, partitions_scanned: 8, partitions_total: 8]",
             err : "",
        },
        TestCase {
            name: "projection-simple-pass",
            plan: (PlanBuilder::from(&source)
                .project(&[col("number")])?
                .build()),
            expect: "\
        Projection: number:UInt64\
        \n  ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000, partitions_scanned: 8, partitions_total: 8]",
        err : "",
        },
        TestCase {
            name: "expression-merge-pass",
            plan: (PlanBuilder::from(&source)
                .expression(&[col("number"), col("number").alias("c1")], "")?
                .expression(&[col("number"), col("number").alias("c2")], "")?
                .build()),
            expect:"Expression: number:UInt64, number as c2:UInt64 ()\
            \n  Expression: number:UInt64, number as c1:UInt64 ()\
            \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000, partitions_scanned: 8, partitions_total: 8]",
            err : "",
        },
        TestCase {
            name: "expression-before-projection-pass",
            plan: (PlanBuilder::from(&source)
                .expression(&[col("number"), col("number").alias("c1")], "Before Projection")?
                .project(&[col("c1")])?
                .build()),
            expect:"\
            Projection: c1:UInt64\
            \n  Expression: number:UInt64, number as c1:UInt64 (Before Projection)\
            \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000, partitions_scanned: 8, partitions_total: 8]",
            err : "",
        },
        TestCase {
            name: "filter-pass",
            plan: (PlanBuilder::from(&source)
                .filter(col("c1").eq(lit(1i64)))?
                .expression(&[col("number"), col("number").alias("c1")], "Before Projection")?
                .project(&[col("c1")])?
                .build()),
            expect:"\
            Projection: c1:UInt64\
            \n  Expression: number:UInt64, number as c1:UInt64 (Before Projection)\
            \n    Filter: (c1 = 1)\
            \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000, partitions_scanned: 8, partitions_total: 8]",
            err : "",
        },
    ];

    for test in tests {
        match &test.plan {
            Ok(plan) => {
                let actual_plan = format!("{:?}", plan);
                assert_eq!(test.expect, actual_plan, "{:#?}", test.name);
            }
            Err(e) => {
                assert_eq!(test.err, e.message(), "{:#?}", test.name);
            }
        }
    }
    Ok(())
}
