// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::prelude::*;

use crate::*;

#[test]
fn test_projection_plan() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]);

    let projection = PlanNode::Projection(ProjectionPlan {
        expr: vec![col("a")],
        schema: DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]),
        input: Arc::from(PlanBuilder::from(&PlanNode::Empty(EmptyPlan { schema })).build()?),
    });
    let _ = projection.schema();
    let expect = "Projection: a:Utf8";
    let actual = format!("{:?}", projection);
    assert_eq!(expect, actual);
    Ok(())
}
