// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_aggregator_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::planners::*;

    let ctx = crate::tests::try_create_context()?;

    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let plan = PlanBuilder::from(
        ctx.clone(),
        &PlanNode::ReadSource(test_source.number_read_source_plan_for_test(10000)?),
    )
    .aggregate_partial(vec![sum(field("number")).alias("sumx")], vec![])?
    .aggregate_final(vec![sum(field("number")).alias("sumx")], vec![])?
    .project(vec![field("sumx")])?
    .build()?;
    let explain = PlanNode::Explain(ExplainPlan {
        typ: DFExplainType::Syntax,
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
