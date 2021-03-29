// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn transform_source_test() -> anyhow::Result<()> {
    use std::sync::Arc;

    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::processors::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx);

    let mut pipeline = Pipeline::create();

    let a = test_source.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(a))?;

    let b = test_source.number_source_transform_for_test(8)?;

    pipeline.add_source(Arc::new(b))?;
    pipeline.merge_processor()?;

    let stream = pipeline.execute().await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(16, rows);
    Ok(())
}
