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

use std::future::Future;
use std::intrinsics::assume;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::tokio::time::sleep;
use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::ProcessorPtr;
use futures_util::future::BoxFuture;
use futures_util::future::Either;
use futures_util::FutureExt;
use log::error;
use log::warn;
use petgraph::prelude::NodeIndex;

use crate::pipelines::executor::CompletedAsyncTask;
use crate::pipelines::executor::QueriesExecutorTasksQueue;
use crate::pipelines::executor::QueryExecutorTasksQueue;
use crate::pipelines::executor::RunningGraph;
use crate::pipelines::executor::WorkersCondvar;

pub enum ExecutorTasksQueue {
    QueryExecutorTasksQueue(Arc<QueryExecutorTasksQueue>),
    QueriesExecutorTasksQueue(Arc<QueriesExecutorTasksQueue>),
}

impl ExecutorTasksQueue {
    pub fn get_finished_notify(&self) -> Arc<WatchNotify> {
        match &self {
            ExecutorTasksQueue::QueryExecutorTasksQueue(executor) => executor.get_finished_notify(),
            ExecutorTasksQueue::QueriesExecutorTasksQueue(executor) => {
                executor.get_finished_notify()
            }
        }
    }
    pub fn active_workers(&self) -> usize {
        match &self {
            ExecutorTasksQueue::QueryExecutorTasksQueue(executor) => executor.active_workers(),
            ExecutorTasksQueue::QueriesExecutorTasksQueue(executor) => executor.active_workers(),
        }
    }
    pub fn is_finished(&self) -> bool {
        match &self {
            ExecutorTasksQueue::QueryExecutorTasksQueue(executor) => executor.is_finished(),
            ExecutorTasksQueue::QueriesExecutorTasksQueue(executor) => executor.is_finished(),
        }
    }
    pub fn completed_async_task(&self, condvar: Arc<WorkersCondvar>, task: CompletedAsyncTask) {
        match &self {
            ExecutorTasksQueue::QueryExecutorTasksQueue(executor) => {
                executor.completed_async_task(condvar, task)
            }
            ExecutorTasksQueue::QueriesExecutorTasksQueue(executor) => {
                executor.completed_async_task(condvar, task)
            }
        }
    }

    pub fn is_queries_executor(&self) -> bool {
        matches!(self, ExecutorTasksQueue::QueriesExecutorTasksQueue(_))
    }
}

pub struct ProcessorAsyncTask {
    worker_id: usize,
    processor_id: NodeIndex,
    queue: Arc<ExecutorTasksQueue>,
    workers_condvar: Arc<WorkersCondvar>,
    instant: Instant,
    last_nanos: usize,
    graph: Arc<RunningGraph>,
    inner: BoxFuture<'static, Result<()>>,
}

impl ProcessorAsyncTask {
    pub fn create<Inner: Future<Output = Result<()>> + Send + 'static>(
        query_id: Arc<String>,
        worker_id: usize,
        processor: ProcessorPtr,
        queue: Arc<ExecutorTasksQueue>,
        workers_condvar: Arc<WorkersCondvar>,
        graph: Arc<RunningGraph>,
        inner: Inner,
    ) -> ProcessorAsyncTask {
        let finished_notify = if queue.is_queries_executor() {
            graph.get_finished_notify()
        } else {
            queue.get_finished_notify()
        };

        let inner = async move {
            let left = Box::pin(inner);
            let right = Box::pin(finished_notify.notified());
            match futures::future::select(left, right).await {
                Either::Left((res, _)) => res,
                Either::Right((_, _)) => Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                )),
            }
        };

        let processor_id = unsafe { processor.id() };
        let processor_name = unsafe { processor.name() };
        let queue_clone = queue.clone();
        let graph_clone = graph.clone();
        let inner = async move {
            let start = Instant::now();
            let mut inner = inner.boxed();
            let mut log_graph = false;

            loop {
                let interval = Box::pin(sleep(Duration::from_secs(5)));
                match futures::future::select(interval, inner).await {
                    Either::Left((_, right)) => {
                        inner = right;
                        let elapsed = start.elapsed();
                        let active_workers = queue_clone.active_workers();
                        match elapsed >= Duration::from_secs(200)
                            && active_workers == 0
                            && !log_graph
                        {
                            false => {
                                warn!(
                                    "Very slow processor async task, query_id:{:?}, processor id: {:?}, name: {:?}, elapsed: {:?}, active sync workers: {:?}",
                                    query_id, processor_id, processor_name, elapsed, active_workers
                                );
                            }
                            true => {
                                log_graph = true;
                                error!(
                                    "Very slow processor async task, query_id:{:?}, processor id: {:?}, name: {:?}, elapsed: {:?}, active sync workers: {:?}, {}",
                                    query_id,
                                    processor_id,
                                    processor_name,
                                    elapsed,
                                    active_workers,
                                    graph_clone.format_graph_nodes()
                                );
                            }
                        };
                    }
                    Either::Right((res, _)) => {
                        return res;
                    }
                }
            }
        };

        let instant = Instant::now();
        ProcessorAsyncTask {
            worker_id,
            processor_id,
            queue,
            workers_condvar,
            last_nanos: instant.elapsed().as_nanos() as usize,
            instant,
            graph,
            inner: inner.boxed(),
        }
    }
}

impl Future for ProcessorAsyncTask {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.queue.is_finished() {
            return Poll::Ready(());
        }

        let last_nanos = self.last_nanos;
        let last_instant = self.instant;
        let inner = self.inner.as_mut();

        let before_poll_nanos = elapsed_nanos(last_instant);
        let wait_nanos = before_poll_nanos - last_nanos;
        Profile::record_usize_profile(ProfileStatisticsName::WaitTime, wait_nanos);

        let poll_res = catch_unwind(move || inner.poll(cx));

        let after_poll_nanos = elapsed_nanos(last_instant);
        Profile::record_usize_profile(
            ProfileStatisticsName::CpuTime,
            after_poll_nanos - before_poll_nanos,
        );

        match poll_res {
            Ok(Poll::Pending) => {
                self.last_nanos = after_poll_nanos;
                Poll::Pending
            }
            Ok(Poll::Ready(res)) => {
                self.queue.completed_async_task(
                    self.workers_condvar.clone(),
                    CompletedAsyncTask::create(
                        self.processor_id,
                        self.worker_id,
                        res,
                        self.graph.clone(),
                    ),
                );
                Poll::Ready(())
            }
            Err(cause) => {
                self.queue.completed_async_task(
                    self.workers_condvar.clone(),
                    CompletedAsyncTask::create(
                        self.processor_id,
                        self.worker_id,
                        Err(cause),
                        self.graph.clone(),
                    ),
                );

                Poll::Ready(())
            }
        }
    }
}

fn elapsed_nanos(instant: Instant) -> usize {
    let nanos = (Instant::now() - instant).as_nanos();
    unsafe {
        assume(nanos < 18446744073709551615_u128);
    }
    nanos as usize
}
