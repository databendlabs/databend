// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_processor_empty() -> crate::error::FuseQueryResult<()> {
    use crate::processors::*;

    let empty = EmptyProcessor::create();

    let expect_name = "EmptyProcessor";
    let actual_name = empty.name();
    assert_eq!(expect_name, actual_name);

    empty.execute().await?;
    Ok(())
}
