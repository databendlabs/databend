// Copyright 2022 Datafuse Labs.
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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::{ErrorCode, Result};
use futures::Stream;
use futures::stream::{Abortable, AbortHandle};
use common_streams::AbortStream;

use crate::pipelines::new::executor::PipelinePullingExecutor;

pub struct ProcessorExecutorStream {
    is_aborted: Arc<Abortable<()>>,
    executor: PipelinePullingExecutor,
}

impl ProcessorExecutorStream {
    pub fn create(mut executor: PipelinePullingExecutor) -> Result<(AbortHandle, Self)> {
        let (handle, reg) = AbortHandle::new_pair();
        let is_aborted = Arc::new(Abortable::new((), reg));

        executor.start();
        Ok((handle, Self { is_aborted, executor }))
    }
}

impl Stream for ProcessorExecutorStream {
    type Item = Result<DataBlock>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let is_aborted = self.is_aborted.clone();
        let self_ = Pin::get_mut(self);
        match self_.executor.try_pull_data(move || is_aborted.is_aborted()) {
            Err(cause) => Poll::Ready(Some(Err(cause))),
            Ok(Some(data)) => Poll::Ready(Some(Ok(data))),
            Ok(None) => match self_.is_aborted.is_aborted() {
                false => Poll::Ready(None),
                true => Poll::Ready(Some(Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed",
                ))))
            },
        }
    }
}
