use std::collections::VecDeque;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use futures::{FutureExt, pin_mut};
use futures::future::BoxFuture;
use futures::task::{ArcWake, WakerRef};

use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::executor::executor_graph::RunningGraph;
use crate::pipelines::new::executor::executor_tasks::{ExecutingAsyncTask, ExecutorTasksQueue};
use crate::pipelines::new::processors::processor::ProcessorPtr;

enum ExecutorWorkerTask {
    None,
    Sync(ProcessorPtr),
    Async(ProcessorPtr),
    AsyncSchedule(BoxFuture<'static, Result<()>>),
}

pub struct ExecutorWorkerContext {
    worker_num: usize,
    task: ExecutorWorkerTask,
}

impl ExecutorWorkerContext {
    pub fn create(worker_num: usize) -> Self {
        ExecutorWorkerContext {
            worker_num,
            task: ExecutorWorkerTask::None,
        }
    }

    pub fn has_task(&self) -> bool {
        !matches!(&self.task, ExecutorWorkerTask::None)
    }

    pub fn get_worker_num(&self) -> usize {
        self.worker_num
    }

    pub fn set_sync_task(&mut self, processor: ProcessorPtr) {
        self.task = ExecutorWorkerTask::Sync(processor)
    }

    pub fn set_async_task(&mut self, processor: ProcessorPtr) {
        self.task = ExecutorWorkerTask::Async(processor)
    }

    pub fn execute_task(&mut self, queue: &ExecutorTasksQueue) -> Result<usize> {
        match std::mem::replace(&mut self.task, ExecutorWorkerTask::None) {
            ExecutorWorkerTask::None => Err(ErrorCode::LogicalError("Execute none task.")),
            ExecutorWorkerTask::Sync(processor) => self.execute_sync_task(processor),
            ExecutorWorkerTask::Async(processor) => self.execute_async_task(processor, queue),
            ExecutorWorkerTask::AsyncSchedule(boxed_future) => self.schedule_async_task(boxed_future, queue),
        }
    }

    fn execute_sync_task(&mut self, processor: ProcessorPtr) -> Result<usize> {
        unsafe {
            (&mut *processor.get()).process()?;
            unimplemented!()
        }
    }

    fn execute_async_task(&mut self, processor: ProcessorPtr, queue: &ExecutorTasksQueue) -> Result<usize> {
        unsafe {
            let mut boxed_future = (&mut *processor.get()).async_process().boxed();
            self.schedule_async_task(boxed_future, queue)
        }
    }

    fn schedule_async_task(&mut self, mut boxed_future: BoxFuture<'static, Result<()>>, queue: &ExecutorTasksQueue) -> Result<usize> {
        loop {
            let finished = Arc::new(AtomicBool::new(false));
            let waker = ExecutingAsyncTaskWaker::create(&finished);

            let waker = futures::task::waker_ref(&waker);
            let mut cx = Context::from_waker(&waker);

            match boxed_future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(res)) => { return unimplemented!(); }
                Poll::Ready(Err(cause)) => { return Err(cause); }
                Poll::Pending => {
                    let async_task = ExecutingAsyncTask { finished: finished.clone(), future: boxed_future };

                    match queue.push_executing_async_task(self.worker_num, async_task) {
                        None => { return unimplemented!(); }
                        Some(task) => { boxed_future = task.future; }
                    };
                }
            };
        }
    }


    pub fn wait_wakeup(&self) {
        // condvar.wait(guard);
    }
}

struct ExecutingAsyncTaskWaker(Arc<AtomicBool>);

impl ExecutingAsyncTaskWaker {
    pub fn create(flag: &Arc<AtomicBool>) -> Arc<ExecutingAsyncTaskWaker> {
        Arc::new(ExecutingAsyncTaskWaker(flag.clone()))
    }
}

impl ArcWake for ExecutingAsyncTaskWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, Ordering::Release);
    }
}

