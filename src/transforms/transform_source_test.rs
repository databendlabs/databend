// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn transform_source_test() -> crate::error::FuseQueryResult<()> {
    use futures::TryStreamExt;
    use std::sync::Arc;

    use crate::processors::*;

    let test_source = crate::tests::NumberTestData::create();
    let ctx =
        crate::contexts::FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;

    let mut pipeline = Pipeline::create();

    let a = test_source.number_source_transform_for_test(ctx.clone(), 8)?;
    pipeline.add_source(Arc::new(a))?;

    let b = test_source.number_source_transform_for_test(ctx, 8)?;

    pipeline.add_source(Arc::new(b))?;
    pipeline.merge_processor()?;

    let stream = pipeline.execute().await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(16, rows);
    Ok(())
}
