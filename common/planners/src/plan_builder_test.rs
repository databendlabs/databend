// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::test::Test;
use crate::*;

#[test]
fn test_plan_builds() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    struct TestCase {
        name: &'static str,
        plan: Result<PlanNode>,
        expect: &'static str
    }

    let source = Test::create().generate_source_plan_for_test(10000)?;
    let tests = vec![
        TestCase {
            name: "field(*)-pass",
            plan: (PlanBuilder::from(&source)
                .expression(&[ExpressionAction::Wildcard], "")?
                .project(&[col("number")])?
                .build()),
            expect: "\
            Projection: number:UInt64\
            \n  Expression: *:UInt64 ()\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
        },
        TestCase {
            name: "constant-alias-pass",
            plan: (PlanBuilder::from(&source)
                .expression(&[add(lit(4), lit(5)).alias("4_5"), add(col("4_5"), lit(2))], "")?
                .project(&[col("4_5")])?
                .build()),
            expect: "\
            Projection: 4_5:Int64\
            \n  Expression: (4 + 5) as 4_5:UInt64, ((4 + 5) + 2):Int64 ()\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
        },
        TestCase {
            name: "projection-simple-pass",
            plan: (PlanBuilder::from(&source)
                .project(&[col("number")])?
                .build()),
            expect: "\
        Projection: number:UInt64\
        \n  ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
        },
        TestCase {
            name: "projection-alias-pass",
            plan: (PlanBuilder::from(&source)
                .project(&[col("number"), col("number").alias("c1")])?
                .build()),
            expect:"\
            Projection: number:UInt64, number as c1:UInt64\
            \n  ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
        },
        TestCase {
            name: "expression-alias-pass",
            plan: (PlanBuilder::from(&source)
                .expression(&[col("number"), col("number").alias("c1")], "desc")?
                .build()),
            expect:"\
            Expression: number:UInt64, number as c1:UInt64 (desc)\
            \n  ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
        },
        TestCase {
            name: "expression-merge-pass",
            plan: (PlanBuilder::from(&source)
                .expression(&[col("number"), col("number").alias("c1")], "")?
                .expression(&[col("number"), col("number").alias("c2")], "")?
                .build()),
            expect:"Expression: number:UInt64, number as c2:UInt64 ()\
            \n  Expression: number:UInt64, number as c1:UInt64 ()\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
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
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
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
            \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
        },
    ];

    for test in tests {
        let plan = test.plan?;
        let actual_plan = format!("{:?}", plan);
        assert_eq!(test.expect, actual_plan, "{:#?}", test.name);
    }
    Ok(())
}
