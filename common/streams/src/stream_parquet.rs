// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::Result;
use crossbeam::channel::Receiver;
use futures::Stream;

pub struct ParquetStream {
    response_rx: Receiver<Option<Result<DataBlock>>>,
}

impl ParquetStream {
    pub fn try_create(response_rx: Receiver<Option<Result<DataBlock>>>) -> Result<Self> {
        Ok(ParquetStream { response_rx })
    }
}

impl Stream for ParquetStream {
    type Item = Result<DataBlock>;

    fn poll_next(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.response_rx.recv() {
            Ok(block) => Poll::Ready(block),
            // RecvError means receiver has exited and closed the channel
            Err(_) => Poll::Ready(None),
        }
    }
}
