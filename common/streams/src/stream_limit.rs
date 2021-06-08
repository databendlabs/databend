// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct LimitStream {
    input: SendableDataBlockStream,
    limit: Option<usize>,
    offset: usize,
    current: usize,
}

impl LimitStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<Self> {
        Ok(LimitStream {
            input,
            limit,
            offset,
            current: 0,
        })
    }

    pub fn limit(&mut self, block: &DataBlock) -> Result<Option<DataBlock>> {
        let rows = block.num_rows();

        // There are two intervals:
        // r1: [current, current + rows)
        // r2: [offset, offset + limit), note that limit may be infinite.
        //
        // There are 6 possible relationships between r1 and r2:
        //
        // TODO: We can clean some codes while RFC 2497-if-let-chains stable.
        // https://github.com/rust-lang/rust/issues/53668
        if self.current + rows <= self.offset {
            // case 1: no overlap. r1.r <= r2.l.
            // output nothing.
            self.current += rows;
            Ok(None)
        } else if self
            .limit
            .map(|limit| self.offset + limit == self.current)
            .unwrap_or(false)
        {
            // case 2: no overlap. r2.r == r1.l.
            // output nothing.
            Ok(None)
        } else if self.current <= self.offset
            && self
                .limit
                .map(|limit| self.current + rows >= self.offset + limit)
                .unwrap_or(false)
        {
            // case 3: r1 contains r2.
            // output r2.
            self.current = self.offset + self.limit.unwrap();
            Ok(Some(block.slice(self.offset, self.limit.unwrap())))
        } else if self.offset <= self.current
            && self
                .limit
                .map(|limit| self.offset + limit >= self.current + rows)
                .unwrap_or(true)
        {
            // case 4: r2 contains r1.
            // output r1.
            self.current += rows;
            Ok(Some(block.clone()))
        } else if self.current <= self.offset {
            // case 5: overlap and r1 in the left. r1.l <= r2.l
            self.current += rows;
            Ok(Some(
                block.slice(self.offset, self.current + rows - self.offset),
            ))
        } else {
            // case 6: overlap and r2 in the left.
            self.current = self.offset + self.limit.unwrap();
            Ok(Some(block.slice(
                self.current,
                self.offset + self.limit.unwrap() - self.current,
            )))
        }
    }
}

impl Stream for LimitStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(ref v)) => self.limit(v).transpose(),
            other => other,
        })
    }
}
