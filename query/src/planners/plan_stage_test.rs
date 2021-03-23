// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_stage_plan() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use crate::planners::*;

    let ctx =
        crate::tests::try_create_context()?.with_id("cf6db5fe-7595-4d85-97ee-71f051b21cbe")?;

    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let plan = PlanBuilder::from(
        ctx.clone(),
        &PlanNode::ReadSource(test_source.number_read_source_plan_for_test(10000)?),
    )
    .aggregate_partial(vec![sum(col("number")).alias("sumx")], vec![])?
    .stage(StageState::AggregatorMerge)?
    .aggregate_final(vec![sum(col("number")).alias("sumx")], vec![])?
    .project(vec![col("sumx")])?
    .build()?;
    let explain = PlanNode::Explain(ExplainPlan {
        typ: DFExplainType::Syntax,
        input: Arc::new(plan),
    });
    let expect = "Projection: sumx:UInt64\
    \n  AggregatorFinal: groupBy=[[]], aggr=[[sum([number]) as sumx]]\
    \n    RedistributeStage[state: AggregatorMerge, id: 0]\
    \n      AggregatorPartial: groupBy=[[]], aggr=[[sum([number]) as sumx]]\
    \n        ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:10000, Read Bytes:80000)";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    Ok(())
}
