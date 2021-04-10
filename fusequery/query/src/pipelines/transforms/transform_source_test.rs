// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn transform_source_test() -> anyhow::Result<()> {
    use std::sync::Arc;

    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx);

    let mut pipeline = Pipeline::create();

    let a = test_source.number_source_transform_for_test(1)?;
    pipeline.add_source(Arc::new(a))?;

    let b = test_source.number_source_transform_for_test(1)?;

    pipeline.add_source(Arc::new(b))?;
    pipeline.merge_processor()?;

    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 0      |",
        "| 0      |",
        "+--------+",
    ];
    crate::assert_blocks_eq!(expected, result.as_slice());

    Ok(())
}
