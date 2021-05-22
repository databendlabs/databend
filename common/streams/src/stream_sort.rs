// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_datablocks::SortColumnDescription;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct SortStream {
    input: SendableDataBlockStream,
    sort_columns_descriptions: Vec<SortColumnDescription>
}

impl SortStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        sort_columns_descriptions: Vec<SortColumnDescription>
    ) -> Result<Self> {
        Ok(SortStream {
            input,
            sort_columns_descriptions
        })
    }
}

impl Stream for SortStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(v)) => Some(DataBlock::sort_block(
                &v,
                &self.sort_columns_descriptions,
                None
            )),
            other => other
        })
    }
}
