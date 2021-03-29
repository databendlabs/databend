// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_processor_empty() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::processors::*;

    let empty = EmptyProcessor::create();

    let expect_name = "EmptyProcessor";
    let actual_name = empty.name();
    assert_eq!(expect_name, actual_name);

    empty.execute().await?;
    Ok(())
}
