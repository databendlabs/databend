// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_processor_merge() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::datavalues::*;
    use crate::processors::*;
    use crate::testdata;

    let test_source = testdata::MemoryTestData::create();
    let mut pipeline = Pipeline::create();

    let mut columns = vec![];
    for k in 0..2 {
        columns.push(k);
    }
    let a = test_source.memory_table_source_transform_for_test(vec![columns])?;
    pipeline.add_source(Arc::new(a))?;

    pipeline.expand_processor(4)?;
    pipeline.merge_processor()?;

    let mut stream = pipeline.execute().await?;
    let v = stream.next().await.unwrap().unwrap();
    let actual = v.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let expect = &Int64Array::from(vec![0, 1]);
    assert_eq!(expect.clone(), actual.clone());
    Ok(())
}
