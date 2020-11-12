// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::stream::Stream;
use std::task::{Context, Poll};

use crate::datablocks::DataBlock;
use crate::error::Result;

pub struct ChunkStream {
    index: usize,
    data: Vec<DataBlock>,
}

impl ChunkStream {
    pub fn create(data: Vec<DataBlock>) -> Self {
        ChunkStream { data, index: 0 }
    }
}

impl Stream for ChunkStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            let idx = self.index;
            self.index += 1;
            Some(Ok(self.data[idx].clone()))
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}
