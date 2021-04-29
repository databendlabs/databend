// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::Result;
use common_progress::Progress;
use common_progress::ProgressCallback;
use futures::Stream;
use pin_project_lite::pin_project;

use crate::SendableDataBlockStream;

pin_project! {
    pub struct ProgressStream {
        #[pin]
        input: SendableDataBlockStream,
        callback: ProgressCallback,
        progress: Progress,
    }
}

impl ProgressStream {
    pub fn try_create(input: SendableDataBlockStream, callback: ProgressCallback) -> Result<Self> {
        Ok(Self {
            input,
            callback,
            progress: Progress::create()
        })
    }
}

impl Stream for ProgressStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.input.poll_next(ctx) {
            Poll::Ready(x) => match x {
                Some(result) => match result {
                    Ok(block) => {
                        this.progress.add_rows(block.num_rows());
                        this.progress.add_bytes(block.memory_size());
                        (this.callback)(&this.progress);
                        Poll::Ready(Some(Ok(block)))
                    }
                    Err(e) => Poll::Ready(Some(Err(e)))
                },
                None => Poll::Ready(None)
            },
            Poll::Pending => Poll::Pending
        }
    }
}
