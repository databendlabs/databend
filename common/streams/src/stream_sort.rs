// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use anyhow::Result;
use common_datablocks::sort_block;
use common_datablocks::DataBlock;
use common_datablocks::SortColumnDescription;
use futures::stream::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct SortStream {
    input: SendableDataBlockStream,
    sort_columns_descriptions: Vec<SortColumnDescription>,
    limit: Option<usize>,
}

impl SortStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        sort_columns_descriptions: Vec<SortColumnDescription>,
        limit: Option<usize>,
    ) -> Result<Self> {
        Ok(SortStream {
            input,
            sort_columns_descriptions,
            limit,
        })
    }
}

impl Stream for SortStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(v)) => Some(sort_block(&v, &self.sort_columns_descriptions, self.limit)),
            other => other,
        })
    }
}
