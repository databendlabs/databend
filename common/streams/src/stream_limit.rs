// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use anyhow::Result;
use common_arrow::arrow;
use common_datablocks::DataBlock;
use futures::stream::Stream;
use futures::stream::StreamExt;

use crate::SendableDataBlockStream;

pub struct LimitStream {
    input: SendableDataBlockStream,
    limit: usize,
    current: usize,
}

impl LimitStream {
    pub fn try_create(input: SendableDataBlockStream, limit: usize) -> Result<Self> {
        Ok(LimitStream {
            input,
            limit,
            current: 0,
        })
    }

    pub fn limit(&mut self, block: &DataBlock) -> Result<Option<DataBlock>> {
        let rows = block.num_rows();
        if self.current == self.limit {
            Ok(None)
        } else if (self.current + rows) < self.limit {
            self.current += rows;
            Ok(Some(block.clone()))
        } else {
            let keep = self.limit - self.current;
            self.current = self.limit;

            let mut limited_columns = Vec::with_capacity(block.num_columns());
            for i in 0..block.num_columns() {
                limited_columns.push(arrow::compute::limit(block.column(i), keep));
            }
            Ok(Some(DataBlock::create(
                block.schema().clone(),
                limited_columns,
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
