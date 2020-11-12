// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[async_std::test]
async fn test_chunk_stream() {
    use async_std::stream::StreamExt;

    use crate::datablocks::DataBlock;
    use crate::streams::ChunkStream;

    let mut s1 = ChunkStream::create(vec![
        DataBlock::empty(),
        DataBlock::empty(),
        DataBlock::empty(),
    ]);
    while let Some(v) = s1.next().await {
        println!("{:?}", v);
    }
}
