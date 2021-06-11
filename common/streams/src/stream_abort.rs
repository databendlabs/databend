// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::{Result, ErrorCode};
use futures::Stream;
use pin_project_lite::pin_project;

use crate::SendableDataBlockStream;
use futures::stream::{Abortable, AbortHandle};

pin_project! {
    pub struct AbortStream {
        #[pin]
        input: Abortable<SendableDataBlockStream>
    }
}

impl AbortStream {
    pub fn try_create(input: SendableDataBlockStream) -> Result<(AbortHandle, Self)> {
        let (handle, reg) = AbortHandle::new_pair();
        Ok((handle, Self { input: Abortable::new(input, reg) }))
    }
}

impl Stream for AbortStream {
    type Item = Result<DataBlock>;

    fn poll_next(self: std::pin::Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let is_aborted = this.input.is_aborted();

        match this.input.poll_next(ctx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(block)) => Poll::Ready(Some(block)),
            Poll::Ready(None) => {
                match is_aborted {
                    true => Poll::Ready(Some(Err(ErrorCode::AbortedQuery("")))),
                    false => Poll::Ready(None)
                }
            }
        }
    }
}
