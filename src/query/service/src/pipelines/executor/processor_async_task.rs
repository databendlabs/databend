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
use std::mem::ManuallyDrop;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use common_base::runtime::catch_unwind;
use common_exception::Result;
use common_pipeline_core::processors::processor::ProcessorPtr;
use petgraph::prelude::NodeIndex;

use crate::pipelines::executor::executor_condvar::WorkersCondvar;
use crate::pipelines::executor::executor_tasks::CompletedAsyncTask;
use crate::pipelines::executor::executor_tasks::ExecutorTasksQueue;

pub struct ProcessorAsyncTask {
    pub worker_id: usize,
    pub processor_id: NodeIndex,
    queue: Arc<ExecutorTasksQueue>,
    workers_condvar: Arc<WorkersCondvar>,
    pending_waker: AtomicPtr<Waker>,
}

impl ProcessorAsyncTask {
    pub fn create(
        worker_id: usize,
        processor_id: NodeIndex,
        queue: Arc<ExecutorTasksQueue>,
        workers_condvar: Arc<WorkersCondvar>,
    ) -> Arc<ProcessorAsyncTask> {
        Arc::new(ProcessorAsyncTask {
            worker_id,
            processor_id,
            queue,
            workers_condvar,
            pending_waker: AtomicPtr::new(std::ptr::null_mut()),
        })
    }

    #[inline(always)]
    pub fn is_finished(&self) -> bool {
        self.queue.is_finished()
    }

    #[inline(always)]
    pub fn finish(self: Arc<Self>, res: Result<()>) -> Poll<()> {
        self.queue.completed_async_task(
            self.workers_condvar.clone(),
            CompletedAsyncTask::create(
                self.processor_id,
                self.worker_id,
                self.pending_waker.load(Ordering::SeqCst) != std::ptr::null_mut(),
                res,
            ),
        );

        Poll::Ready(())
    }

    pub fn wakeup(&self) {
        let waker = self
            .pending_waker
            .swap(std::ptr::null_mut(), Ordering::SeqCst);
        if !waker.is_null() {
            unsafe {
                let waker = std::ptr::read(waker as *const Waker);
                waker.wake();
            }
        }
    }

    #[inline(always)]
    pub fn set_pending_watcher(self: Arc<Self>, waker: Waker) -> Poll<()> {
        let mut expected = std::ptr::null_mut();
        let desired = Box::into_raw(Box::new(waker));

        loop {
            match self.pending_waker.compare_exchange_weak(
                expected,
                desired,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Err(new_expected) => unsafe {
                    if !new_expected.is_null() && (&*new_expected).will_wake(&*desired) {
                        return Poll::Pending;
                    }

                    expected = new_expected;
                },
                Ok(old_value) => {
                    if !old_value.is_null() {
                        unsafe { drop(Box::from_raw(old_value)) };
                    }

                    self.queue.pending_async_task(&self);
                    return Poll::Pending;
                }
            }
        }
    }
}

pub struct ProcessorAsyncFuture<Inner: Future<Output = Result<()>> + Send + 'static> {
    inner: Inner,
    task: Arc<ProcessorAsyncTask>,
}

impl<Inner: Future<Output = Result<()>> + Send + 'static> ProcessorAsyncFuture<Inner> {
    pub fn create(
        _query_id: Arc<String>,
        worker_id: usize,
        processor: ProcessorPtr,
        queue: Arc<ExecutorTasksQueue>,
        workers_condvar: Arc<WorkersCondvar>,
        inner: Inner,
    ) -> ProcessorAsyncFuture<Inner> {
        ProcessorAsyncFuture {
            inner,
            task: ProcessorAsyncTask::create(
                worker_id,
                unsafe { processor.id() },
                queue,
                workers_condvar,
            ),
        }
    }
}

impl<Inner: Future<Output = Result<()>> + Send + 'static> Future for ProcessorAsyncFuture<Inner> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.task.is_finished() {
            return Poll::Ready(());
        }

        let task = self.task.clone();
        let inner = unsafe { self.map_unchecked_mut(|x| &mut x.inner) };

        match catch_unwind(move || (inner.poll(cx), cx)) {
            Ok((Poll::Ready(res), _)) => task.finish(res),
            Err(cause) => task.finish(Err(cause)),
            Ok((Poll::Pending, cx)) => task.set_pending_watcher(cx.waker().clone()),
        }
    }
}
