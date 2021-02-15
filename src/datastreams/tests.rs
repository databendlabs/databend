// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_chunk_stream() {
    use futures::stream::StreamExt;
    use std::sync::Arc;

    use crate::datablocks::*;
    use crate::datastreams::*;
    use crate::datavalues::*;

    let mut s1 = DataBlockStream::create(
        Arc::new(DataSchema::empty()),
        None,
        vec![DataBlock::empty(), DataBlock::empty(), DataBlock::empty()],
    );
    while let Some(_) = s1.next().await {}
}
