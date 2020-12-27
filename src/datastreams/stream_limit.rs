// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::task::{Context, Poll};

use futures::stream::{Stream, StreamExt};

use crate::datablocks::DataBlock;
use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;

pub struct LimitStream {
    input: SendableDataBlockStream,
    limit: usize,
    current: usize,
}

impl LimitStream {
    pub fn try_create(input: SendableDataBlockStream, limit: usize) -> FuseQueryResult<Self> {
        Ok(LimitStream {
            input,
            limit,
            current: 0,
        })
    }

    pub fn limit(&mut self, block: &DataBlock) -> FuseQueryResult<Option<DataBlock>> {
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
    type Item = FuseQueryResult<DataBlock>;

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
