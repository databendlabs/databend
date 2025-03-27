// Copyright 2021 Datafuse Labs
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
use std::task::Context;
use std::task::Poll;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use futures::Stream;

use crate::pipelines::executor::PipelinePullingExecutor;

pub struct PullingExecutorStream {
    end_of_stream: bool,
    executor: Option<PipelinePullingExecutor>,
}

impl PullingExecutorStream {
    pub fn create(mut executor: PipelinePullingExecutor) -> Result<Self> {
        executor.start();
        Ok(Self {
            end_of_stream: false,
            executor: Some(executor),
        })
    }

    fn poll_next_impl(&mut self) -> Poll<Option<Result<DataBlock>>> {
        if let Some(mut executor) = self.executor.take() {
            return match executor.pull_data() {
                Err(cause) => {
                    self.end_of_stream = true;
                    drop(executor);
                    Poll::Ready(Some(Err(cause)))
                }
                Ok(Some(data)) => {
                    self.executor = Some(executor);
                    Poll::Ready(Some(Ok(data)))
                }
                Ok(None) => {
                    self.end_of_stream = true;
                    drop(executor);
                    Poll::Ready(None)
                }
            };
        }

        Poll::Ready(None)
    }
}

impl Stream for PullingExecutorStream {
    type Item = Result<DataBlock>;

    // The ctx can't be wake up, so we can't return Poll::Pending here
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_ = Pin::get_mut(self);
        if self_.end_of_stream {
            return Poll::Ready(None);
        }

        self_.poll_next_impl()
    }
}
