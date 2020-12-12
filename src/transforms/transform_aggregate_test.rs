// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_projection() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::datavalues::*;
    use crate::planners::{self, *};
    use crate::processors::*;
    use crate::testdata;
    use crate::transforms::*;

    let test_source = testdata::NumberTestData::create();
    let mut pipeline = Pipeline::create();

    let a = test_source.number_source_transform_for_test(16)?;
    pipeline.add_source(Arc::new(a))?;

    if let PlanNode::Aggregate(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .aggregate(
            vec![],
            vec![planners::add(
                ExpressionPlan::Function {
                    op: "sum".to_string(),
                    args: vec![planners::field("number")],
                },
                planners::constant(2u64),
            )],
        )?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(AggregatePartialTransform::try_create(
                plan.schema.clone(),
                plan.aggr_expr.clone(),
            )?))
        })?;
        pipeline.merge_processor()?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(AggregateFinalTransform::try_create(
                plan.schema.clone(),
                plan.aggr_expr.clone(),
            )?))
        })?;
    }
    let mut stream = pipeline.execute().await?;
    while let Some(v) = stream.next().await {
        let v = v?;
        if v.num_rows() > 0 {
            let actual = v.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
            let expect = &UInt64Array::from(vec![122]);
            assert_eq!(expect.clone(), actual.clone());
        }
    }
    Ok(())
}
