// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_projection_plan() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use crate::datavalues::*;
    use crate::planners::*;

    let ctx = crate::tests::try_create_context()?;

    let projection = PlanNode::Projection(ProjectionPlan {
        expr: vec![col("a")],
        schema: Arc::new(DataSchema::new(vec![DataField::new(
            "a",
            DataType::Utf8,
            false,
        )])),
        input: Arc::from(PlanBuilder::empty(ctx).build()?),
    });
    let _ = projection.schema();
    let expect = "Projection: a:Utf8";
    let actual = format!("{:?}", projection);
    assert_eq!(expect, actual);
    Ok(())
}
