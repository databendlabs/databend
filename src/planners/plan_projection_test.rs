// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[test]
fn test_projection_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::datavalues::*;
    use crate::planners::*;

    let ctx = crate::sessions::FuseQueryContext::try_create_ctx()?;

    let projection = PlanNode::Projection(ProjectionPlan {
        expr: vec![field("a")],
        schema: Arc::new(DataSchema::new(vec![DataField::new(
            "a",
            DataType::Utf8,
            false,
        )])),
        input: Arc::from(PlanBuilder::empty(ctx).build()?),
    });
    let _ = projection.schema();
    let expect = "Projection: a:Utf8\n  ";
    let actual = format!("{:?}", projection);
    assert_eq!(expect, actual);
    Ok(())
}
