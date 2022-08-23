// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
