// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[async_std::test]
async fn test_chunk_stream() {
    use async_std::stream::StreamExt;
    use std::sync::Arc;

    use crate::datablocks::*;
    use crate::datastreams::*;
    use crate::datavalues::*;

    let mut s1 = MemoryStream::create(
        Arc::new(DataSchema::empty()),
        None,
        vec![DataBlock::empty(), DataBlock::empty(), DataBlock::empty()],
    );
    while let Some(_) = s1.next().await {}
}
