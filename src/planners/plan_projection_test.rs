// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_projection_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::datavalues::*;
    use crate::planners::*;

    let projection = PlanNode::Projection(ProjectionPlan {
        expr: vec![ExpressionPlan::Field("a".to_string())],
        schema: Arc::new(DataSchema::new(vec![DataField::new(
            "a",
            DataType::Utf8,
            false,
        )])),
        input: Arc::from(PlanBuilder::empty().build()?),
    });
    let _ = projection.schema();
    let expect = "└─ Projection: a:Utf8";
    let actual = format!("{:?}", projection);
    assert_eq!(expect, actual);
    Ok(())
}
