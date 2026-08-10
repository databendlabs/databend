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
use futures::Future;
use futures::Stream;

use crate::pipelines::executor::PipelinePullingExecutor;

type PullDataFuture =
    Pin<Box<dyn Future<Output = (PipelinePullingExecutor, Result<Option<DataBlock>>)> + Send>>;

pub struct PullingExecutorStream {
    end_of_stream: bool,
    executor: Option<PipelinePullingExecutor>,
    pull_data_future: Option<PullDataFuture>,
}

impl Unpin for PullingExecutorStream {}

impl PullingExecutorStream {
    pub fn create(mut executor: PipelinePullingExecutor) -> Result<Self> {
        executor.start();
        Ok(Self {
            end_of_stream: false,
            executor: Some(executor),
            pull_data_future: None,
        })
    }

    fn poll_next_impl(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<DataBlock>>> {
        if self.pull_data_future.is_none()
            && let Some(mut executor) = self.executor.take()
        {
            self.pull_data_future = Some(Box::pin(async move {
                let res = executor.pull_data().await;
                (executor, res)
            }));
        }

        if let Some(future) = self.pull_data_future.as_mut() {
            return match future.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready((executor, Err(cause))) => {
                    self.pull_data_future = None;
                    self.end_of_stream = true;
                    drop(executor);
                    Poll::Ready(Some(Err(cause)))
                }
                Poll::Ready((executor, Ok(Some(data)))) => {
                    self.pull_data_future = None;
                    self.executor = Some(executor);
                    Poll::Ready(Some(Ok(data)))
                }
                Poll::Ready((executor, Ok(None))) => {
                    self.pull_data_future = None;
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_ = Pin::get_mut(self);
        if self_.end_of_stream {
            return Poll::Ready(None);
        }

        self_.poll_next_impl(cx)
    }
}
