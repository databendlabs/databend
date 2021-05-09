// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod tests {
    #[test]
    fn test_aggregator_plan() -> anyhow::Result<()> {
        use std::sync::Arc;

        use pretty_assertions::assert_eq;

        use crate::*;

        let source = Test::create().generate_source_plan_for_test(10000)?;
        let plan = PlanBuilder::from(&source)
            .aggregate_partial(&[sum(col("number")).alias("sumx")], &[])?
            .aggregate_final(&[sum(col("number")).alias("sumx")], &[])?
            .project(&[col("sumx")])?
            .build()?;
        let explain = PlanNode::Explain(ExplainPlan {
            typ: ExplainType::Syntax,
            input: Arc::new(plan)
        });
        let expect = "\
        Projection: sumx:UInt64\
        \n  AggregatorFinal: groupBy=[[]], aggr=[[sum([number]) as sumx]]\
        \n    AggregatorPartial: groupBy=[[]], aggr=[[sum([number]) as sumx]]\
        \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]";
        let actual = format!("{:?}", explain);
        assert_eq!(expect, actual);
        Ok(())
    }

    #[test]
    fn test_aggregate_expr_check() -> anyhow::Result<()> {
        use anyhow::anyhow;
        use common_exception::ErrorCodes;
        use pretty_assertions::assert_eq;

        use crate::*;

        struct TestCase {
            name: &'static str,
            plan: Result<PlanBuilder, ErrorCodes>,
            expect_error: bool,
            expect: &'static str
        }

        let source = Test::create().generate_source_plan_for_test(10000)?;

        let tests = vec![
            TestCase {
                name: "aggr-expr-check-with-alias",
                plan: (PlanBuilder::from(&source).aggregate_partial(
                    vec![sum(col("number")).alias("a"), add(col("number"), lit(1))],
                    vec![col("a")]
                )),
                expect_error: true,
                expect:
                    "Code: 26, displayText = Expression (number + 1) is not an aggregate function.."
            },
            TestCase {
                name: "aggr-expr-not-in-group-by",
                plan: (PlanBuilder::from(&source).aggregate_partial(
                    vec![
                        sum(col("number")).alias("a"),
                        modular(col("number"), lit(3)),
                    ],
                    vec![modular(col("a"), lit(4))]
                )),
                expect_error: true,
                expect:
                    "Code: 26, displayText = Expression (number % 3) is not an aggregate function.."
            },
            TestCase {
                name: "aggr-expr-valid",
                plan: (PlanBuilder::from(&source).aggregate_partial(
                    vec![
                        sum(col("number")).alias("a"),
                        avg(modular(col("number"), lit(3))),
                    ],
                    vec![modular(col("a"), lit(4))]
                )),
                expect_error: false,
                expect: "AggregatorPartial: groupBy=[[(a % 4)]], aggr=[[sum([number]) as a, avg([(number % 3)])]]\
                         \n  ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]"
            },
        ];

        for test in tests {
            println!("name: {}", test.name);
            match test.plan {
                Err(e) => {
                    if test.expect_error {
                        let actual = format!("{}", e);
                        assert_eq!(test.expect, actual);
                    } else {
                        return Err(anyhow!("Error: we expect a failure."));
                    }
                }
                Ok(p) => {
                    if !test.expect_error {
                        println!("p: {:?}", p.build()?);
                        let actual = format!("{:?}", p.build()?);
                        assert_eq!(test.expect, actual);
                    } else {
                        return Err(anyhow!("Error: we expect a failure."));
                    }
                }
            }
        }
        Ok(())
    }
}
