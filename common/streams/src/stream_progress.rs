// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::Result;
use common_progress::ProgressCallback;
use common_progress::ProgressValues;
use futures::Stream;
use pin_project_lite::pin_project;

use crate::SendableDataBlockStream;

pin_project! {
    pub struct ProgressStream {
        #[pin]
        input: SendableDataBlockStream,
        callback: ProgressCallback,
    }
}

impl ProgressStream {
    pub fn try_create(input: SendableDataBlockStream, callback: ProgressCallback) -> Result<Self> {
        Ok(Self { input, callback })
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
                        let progress_values = ProgressValues {
                            read_rows: block.num_rows(),
                            read_bytes: block.memory_size(),
                            total_rows_to_read: 0
                        };

                        (this.callback)(&progress_values);
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
