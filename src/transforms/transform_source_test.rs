// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn transform_source_test() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::processors::*;
    use crate::testdata;

    let testdata = testdata::NumberTestData::create();
    let mut pipeline = Pipeline::create();

    let a = testdata.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(a))?;

    let b = testdata.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(b))?;
    pipeline.merge_processor()?;

    let mut stream = pipeline.execute().await?;

    let mut rows = 0;
    while let Some(v) = stream.next().await {
        let row = v?.num_rows();
        rows += row;
    }
    assert_eq!(16, rows);
    Ok(())
}
