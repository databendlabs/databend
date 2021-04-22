// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_explain_plan() -> anyhow::Result<()> {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use crate::*;

    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .project(vec![col("number").alias("c1"), col("number").alias("c2")])?
        .filter(add(col("number"), lit(1)).eq(lit(4)))?
        .build()?;
    let explain = PlanNode::Explain(ExplainPlan {
        typ: ExplainType::Syntax,
        input: Arc::new(plan)
    });
    let expect ="Filter: ((number + 1) = 4)\
    \n  Projection: number as c1:UInt64, number as c2:UInt64\
    \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    Ok(())
}
