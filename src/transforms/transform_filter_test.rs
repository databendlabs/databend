// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_filter() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::datavalues::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::testdata;
    use crate::transforms::*;

    let test_source = testdata::NumberTestData::create();
    let mut pipeline = Pipeline::create();

    let a = test_source.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(a))?;

    if let PlanNode::Filter(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .filter(field("number").eq(constant(1)))?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(FilterTransform::try_create(
                plan.predicate.clone(),
            )?))
        })?;
    }
    pipeline.merge_processor()?;

    let mut stream = pipeline.execute().await?;
    while let Some(v) = stream.next().await {
        let v = v?;
        if v.num_rows() > 0 {
            let actual = v.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let expect = &Int64Array::from(vec![1]);
            assert_eq!(expect.clone(), actual.clone());
        }
    }
    Ok(())
}
