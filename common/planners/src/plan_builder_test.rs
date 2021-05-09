// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_plan_builds() -> anyhow::Result<()> {
    use common_exception::Result;
    use pretty_assertions::assert_eq;

    use crate::*;

    struct TestCase {
        name: &'static str,
        plan: Result<PlanNode>,
        expect: &'static str
    }

    let source = Test::create().generate_source_plan_for_test(10000)?;
    let tests = vec![
        TestCase {
            name: "projection-simple-pass",
            plan: (PlanBuilder::from(&source)
                .project(vec![col("number")])?
                .build()),
            expect: "\
        Projection: number:UInt64\
        \n  ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
        },
        TestCase {
            name: "projection-alias-pass",
            plan: (PlanBuilder::from(&source)
                .project(vec![col("number"), col("number").alias("c1")])?
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
            Expression: number:UInt64, c1:UInt64 (desc)\
            \n  ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
        },
        TestCase {
            name: "expression-merge-pass",
            plan: (PlanBuilder::from(&source)
                .expression(&[col("number"), col("number").alias("c1")], "")?
                .expression(&[col("number"), col("number").alias("c2")], "")?
                .build()),
            expect:"Expression: number:UInt64, c1:UInt64, c2:UInt64 ()\
            \n  Expression: number:UInt64, c1:UInt64 ()\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
        },
        TestCase {
            name: "expression-before-projection-pass",
            plan: (PlanBuilder::from(&source)
                .expression(&[col("number"), col("number").alias("c1")], "Before Projection")?
                .project(vec![col("c1")])?
                .build()),
            expect:"\
            Projection: c1:UInt64\
            \n  Expression: number:UInt64, c1:UInt64 (Before Projection)\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
        },
        TestCase {
            name: "filter-pass",
            plan: (PlanBuilder::from(&source)
                .filter(col("c1").eq(lit(1i64)))?
                .expression(&[col("number"), col("number").alias("c1")], "Before Projection")?
                .project(vec![col("c1")])?
                .build()),
            expect:"\
            Projection: c1:UInt64\
            \n  Expression: number:UInt64, c1:UInt64 (Before Projection)\
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
