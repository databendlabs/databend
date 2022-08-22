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

use std::sync::Arc;

use common_exception::Result;
use common_planners::*;
use pretty_assertions::assert_eq;

use crate::test::Test;

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
        \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000, partitions_scanned: 8, partitions_total: 8]";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    Ok(())
}
