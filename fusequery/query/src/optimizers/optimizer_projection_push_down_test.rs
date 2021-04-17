// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_projection_push_down_optimizer() -> anyhow::Result<()> {
    use std::sync::Arc;

    use crate::optimizers::*;
    use common_planners::*;
    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    let ctx = crate::tests::try_create_context()?;

    let plan = PlanNode::Projection(ProjectionPlan {
        expr: vec![col("a"), col("c")],
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
    Projection: (number + 1) as c1:UInt64, number as c2:UInt64\
    \n  Filter: ((((number + 1) + number) + 1) = 1)\
    \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}
