// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pipe() -> anyhow::Result<()> {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use crate::processors::*;

    let mut pipe = Pipe::create();
    let empty = Arc::new(EmptyProcessor::create());
    pipe.add(empty.clone());

    let num = pipe.nums();
    assert_eq!(1, num);

    let get_empty = pipe.processor_by_index(0);
    assert_eq!(empty.name(), get_empty.name());

    let first = pipe.first();
    assert_eq!(empty.name(), first.name());

    let processors = pipe.processors();
    assert_eq!(empty.name(), processors[0].name());

    Ok(())
}
