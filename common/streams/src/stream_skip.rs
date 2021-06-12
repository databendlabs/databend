// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct SkipStream {
    input: SendableDataBlockStream,
    n: usize,
    current: usize,
}

impl SkipStream {
    pub fn new(input: SendableDataBlockStream, n: usize) -> Self {
        SkipStream {
            input,
            n,
            current: 0,
        }
    }
}

impl Stream for SkipStream {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(ref block)) => Some(Ok({
                let rows = block.num_rows();
                if self.current + rows <= self.n {
                    self.current += rows;
                    DataBlock::empty_with_schema(block.schema().clone())
                } else if self.current == self.n {
                    block.clone()
                } else {
                    let keep = self.current + rows - self.n;
                    self.current = self.n;
                    block.slice(keep, rows - keep)
                }
            })),
            other => other,
        })
    }
}
