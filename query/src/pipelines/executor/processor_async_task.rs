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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline::processors::processor::ProcessorPtr;

use crate::pipelines::executor::executor_condvar::WorkersCondvar;
use crate::pipelines::executor::executor_tasks::CompletedAsyncTask;
use crate::pipelines::executor::executor_tasks::ExecutorTasksQueue;

pub struct ProcessorAsyncTask<Inner: Future<Output = Result<()>>> {
    worker_id: usize,
    inner: Pin<Box<Inner>>,
    processor: ProcessorPtr,
    queue: Arc<ExecutorTasksQueue>,
    workers_condvar: Arc<WorkersCondvar>,
}

impl<Inner: Future<Output = Result<()>>> ProcessorAsyncTask<Inner> {
    pub fn create(
        worker_id: usize,
        processor: ProcessorPtr,
        queue: Arc<ExecutorTasksQueue>,
        workers_condvar: Arc<WorkersCondvar>,
        inner: Inner,
    ) -> ProcessorAsyncTask<Inner> {
        ProcessorAsyncTask::<Inner> {
            queue,
            worker_id,
            processor,
            workers_condvar,
            inner: Box::pin(inner),
        }
    }
}

impl<Inner: Future<Output = Result<()>>> Future for ProcessorAsyncTask<Inner> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.inner.as_mut();

        let try_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || -> Poll<Result<()>> {
                inner.poll(cx)
            }));

        match try_result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(res)) => {
                self.queue.completed_async_task(
                    self.workers_condvar.clone(),
                    CompletedAsyncTask::create(self.processor.clone(), self.worker_id, res),
                );

                Poll::Ready(())
            }
            Err(cause) => {
                let res = match cause.downcast_ref::<&'static str>() {
                    None => match cause.downcast_ref::<String>() {
                        None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                        Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                    },
                    Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                };

                self.queue.completed_async_task(
                    self.workers_condvar.clone(),
                    CompletedAsyncTask::create(self.processor.clone(), self.worker_id, res),
                );

                Poll::Ready(())
            }
        }
    }
}
