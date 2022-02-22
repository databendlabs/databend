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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::*;
use pretty_assertions::assert_eq;

use crate::test::Test;

#[test]
fn test_explain_plan() -> Result<()> {
    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .project(&[col("number").alias("c1"), col("number").alias("c2")])?
        .filter(add(col("number"), lit(1)).eq(lit(4)))?
        .having(add(col("number"), lit(1)).eq(lit(4)))?
        .build()?;
    let explain = PlanNode::Explain(ExplainPlan {
        typ: ExplainType::Syntax,
        input: Arc::new(plan),
    });
    let expect ="\
    Having: ((number + 1) = 4)\
    \n  Filter: ((number + 1) = 4)\
    \n    Projection: number as c1:UInt64, number as c2:UInt64\
    \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000, partitions_scanned: 8, partitions_total: 8]";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    assert_eq!(explain.schema().fields().clone(), vec![DataField::new(
        "explain",
        Vu8::to_data_type()
    )]);

    Ok(())
}
