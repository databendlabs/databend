// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_processor_merge() -> crate::error::FuseQueryResult<()> {
    use futures::stream::StreamExt;
    use std::sync::Arc;

    use crate::datavalues::*;
    use crate::processors::*;
    use crate::tests;

    let ctx = crate::tests::try_create_context()?;
    let test_source = tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create();

    let a = test_source.number_source_transform_for_test(2)?;
    pipeline.add_source(Arc::new(a))?;

    pipeline.merge_processor()?;

    let mut stream = pipeline.execute().await?;
    let v = stream.next().await.unwrap().unwrap();
    let actual = v.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
    let expect = &UInt64Array::from(vec![0, 1]);
    assert_eq!(expect.clone().values(), actual.clone().values());
    Ok(())
}
