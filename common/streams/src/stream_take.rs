// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::option::Option::None;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct TakeStream {
    input: SendableDataBlockStream,
    remaining: usize,
}

impl TakeStream {
    pub fn new(input: SendableDataBlockStream, n: usize) -> Self {
        TakeStream {
            input,
            remaining: n,
        }
    }
}

impl Stream for TakeStream {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(ref block)) => {
                let rows = block.num_rows();
                if self.remaining == 0 {
                    None
                } else if self.remaining >= rows {
                    self.remaining -= rows;
                    Some(block.clone())
                } else {
                    let remaining = self.remaining;
                    self.remaining = 0;
                    Some(block.slice(0, remaining))
                }
            }
            .map(Ok),
            other => other,
        })
    }
}
