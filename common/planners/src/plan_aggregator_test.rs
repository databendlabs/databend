// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::test::Test;
use crate::*;

#[test]
fn test_aggregator_plan() -> Result<()> {
    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .aggregate_partial(&[sum(col("number")).alias("sumx")], &[])?
        .aggregate_final(source.schema(), &[sum(col("number")).alias("sumx")], &[])?
        .project(&[col("sumx")])?
        .build()?;
    let explain = PlanNode::Explain(ExplainPlan {
        typ: ExplainType::Syntax,
        input: Arc::new(plan),
    });
    let expect = "\
        Projection: sumx:UInt64\
        \n  AggregatorFinal: groupBy=[[]], aggr=[[sum(number) as sumx]]\
        \n    AggregatorPartial: groupBy=[[]], aggr=[[sum(number) as sumx]]\
        \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    Ok(())
}
