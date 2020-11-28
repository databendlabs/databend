// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_projection() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::planners::*;
    use crate::processors::*;
    use crate::testdata;
    use crate::transforms::*;

    let test_source = testdata::CsvTestData::create();
    let mut pipeline = Pipeline::create();

    let a = test_source.csv_table_source_transform_for_test()?;
    pipeline.add_source(Arc::new(a))?;

    pipeline.add_simple_transform(|| {
        Ok(Box::new(ProjectionTransform::try_create(vec![
            ExpressionPlan::Field("c6".to_string()),
        ])?))
    })?;

    println!("{:?}", pipeline);
    let mut stream = pipeline.execute().await?;
    let v = stream.next().await.unwrap().unwrap();
    let actual = v.num_columns();
    let expect = 1;
    assert_eq!(expect, actual);
    Ok(())
}
