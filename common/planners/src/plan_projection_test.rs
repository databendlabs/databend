// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_projection_plan() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    use crate::*;

    let projection = PlanNode::Projection(ProjectionPlan {
        expr: vec![col("a")],
        schema: Arc::new(DataSchema::new(vec![DataField::new(
            "a",
            DataType::Utf8,
            false
        )])),
        input: Arc::from(PlanBuilder::empty().build()?)
    });
    let _ = projection.schema();
    let expect = "Projection: a:Utf8";
    let actual = format!("{:?}", projection);
    assert_eq!(expect, actual);
    Ok(())
}
