// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_projection_push_down_optimizer() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::optimizers::*;

    let ctx = crate::tests::try_create_context()?;

    let plan = PlanNode::Projection(ProjectionPlan {
        expr: vec![
            col("a"),
            col("b"),
            col("c"),
        ],
        schema: Arc::new(DataSchema::new(vec![
            DataField::new("a", DataType::Utf8, false),
            DataField::new("b", DataType::Utf8, false),
            DataField::new("c", DataType::Utf8, false),
        ])),
        input: Arc::from(PlanBuilder::empty().build()?),
    });

    let mut projection_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = projection_push_down.optimize(&plan)?;

    let expect = "\
    Projection: a:Utf8, b:Utf8, c:Utf8";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}
