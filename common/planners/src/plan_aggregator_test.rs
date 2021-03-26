// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_aggregator_plan() -> crate::error::PlannerResult<()> {
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
        typ: DfExplainType::Syntax,
        input: Arc::new(plan),
    });
    let expect = "Projection: sumx:UInt64\
    \n  AggregatorFinal: groupBy=[[]], aggr=[[sum([number]) as sumx]]\
    \n    AggregatorPartial: groupBy=[[]], aggr=[[sum([number]) as sumx]]\
    \n      ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:10000, Read Bytes:80000)";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    Ok(())
}
