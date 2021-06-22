// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::stream::AbortHandle;
use futures::stream::Abortable;
use futures::Stream;
use pin_project_lite::pin_project;

use crate::SendableDataBlockStream;

pin_project! {
    pub struct AbortStream {
        #[pin]
        input: Abortable<SendableDataBlockStream>
    }
}

impl AbortStream {
    pub fn try_create(input: SendableDataBlockStream) -> Result<(AbortHandle, Self)> {
        let (handle, reg) = AbortHandle::new_pair();
        Ok((handle, Self {
            input: Abortable::new(input, reg),
        }))
    }
}

impl Stream for AbortStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let is_aborted = this.input.is_aborted();

        match this.input.poll_next(ctx) {
            Poll::Ready(None) => match is_aborted {
                false => Poll::Ready(None),
                true => Poll::Ready(Some(Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed",
                )))),
            },
            other => other,
        }
    }
}
