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
            .aggregate_partial(vec![sum(col("number")).alias("sumx")], vec![])?
            .aggregate_final(vec![sum(col("number")).alias("sumx")], vec![])?
            .project(vec![col("sumx")])?
            .build()?;
        let explain = PlanNode::Explain(ExplainPlan {
            typ: ExplainType::Syntax,
            input: Arc::new(plan)
        });
        let expect = "Projection: sumx:UInt64\
        \n  AggregatorFinal: groupBy=[[]], aggr=[[sum([number]) as sumx]]\
        \n    AggregatorPartial: groupBy=[[]], aggr=[[sum([number]) as sumx]]\
        \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]";
        let actual = format!("{:?}", explain);
        assert_eq!(expect, actual);
        Ok(())
    }

    #[test]
    fn test_aggregate_expr_check_1() -> anyhow::Result<()> {
        use anyhow::anyhow;
        use pretty_assertions::assert_eq;

        use crate::*;

        let source = Test::create().generate_source_plan_for_test(10000)?;

        // select sum(number) as a, number+1 group by a
        let plan = PlanBuilder::from(&source).aggregate_partial(
            vec![sum(col("number")).alias("a"), add(col("number"), lit(1))],
            vec![col("a")]
        );
        match plan {
            Err(e) => {
                println!("error: {}", e);
                let actual = format!("{}", e);
                let expect = "Code: 1000, displayText = Expression (number + 1) is not an aggregate function..";
                assert_eq!(expect, actual);
                Ok(())
            }
            Ok(_p) => Err(anyhow!("Error: we expect a failure."))
        }
    }

    #[test]
    fn test_aggregate_expr_check_2() -> anyhow::Result<()> {
        use anyhow::anyhow;
        use pretty_assertions::assert_eq;

        use crate::*;

        let source = Test::create().generate_source_plan_for_test(10000)?;

        // select sum(number) as a, number % 3 group by  number % 4
        let plan = PlanBuilder::from(&source).aggregate_partial(
            vec![
                sum(col("number")).alias("a"),
                modular(col("number"), lit(3)),
            ],
            vec![modular(col("a"), lit(4))]
        );
        match plan {
            Err(e) => {
                println!("error: {}", e);
                let actual = format!("{}", e);
                let expect = "Code: 1000, displayText = Expression (number % 3) is not an aggregate function..";
                assert_eq!(expect, actual);
                Ok(())
            }
            Ok(_p) => Err(anyhow!("Error: we expect a failure."))
        }
    }
}
