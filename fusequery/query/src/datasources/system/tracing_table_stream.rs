// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::Result;
use futures::Stream;

pub struct TracingTableStream {
    log_dir: String,
}

impl TracingTableStream {
    pub fn try_create(log_dir: String) -> Result<Self> {
        Ok(TracingTableStream { log_dir })
    }

    pub fn try_get_one_block(&self) -> Result<Option<DataBlock>> {
        let _dir = self.log_dir.clone();
        todo!()
    }
}

impl Stream for TracingTableStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let block = self.try_get_one_block()?;
        Poll::Ready(block.map(Ok))
    }
}
