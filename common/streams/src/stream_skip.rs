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
    remaining: usize,
}

impl SkipStream {
    pub fn new(input: SendableDataBlockStream, n: usize) -> Self {
        SkipStream {
            input,
            remaining: n,
        }
    }
}

impl Stream for SkipStream {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while self.remaining > 0 {
            match self.input.poll_next_unpin(ctx) {
                Poll::Ready(Some(Ok(ref block))) => {
                    let rows = block.num_rows();
                    if self.remaining >= rows {
                        self.remaining -= rows;
                        continue;
                    } else if self.remaining < rows {
                        let remaining = self.remaining;
                        self.remaining = 0;
                        return Poll::Ready(Some(Ok(block.slice(remaining, rows - remaining))));
                    }
                }
                other => return other,
            }
        }
        self.input.poll_next_unpin(ctx)
    }
}
