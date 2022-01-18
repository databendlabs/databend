use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use futures::task::{ArcWake};
use petgraph::prelude::NodeIndex;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::executor::executor_tasks::{ExecutingAsyncTask, ExecutorTasksQueue};
use crate::pipelines::new::processors::processor::ProcessorPtr;

pub enum ExecutorTask {
    None,
    Sync(ProcessorPtr),
    Async(ProcessorPtr),
    AsyncSchedule(ExecutingAsyncTask),
}

pub struct ExecutorWorkerContext {
    worker_num: usize,
    task: ExecutorTask,
}

impl ExecutorWorkerContext {
    pub fn create(worker_num: usize) -> Self {
        ExecutorWorkerContext {
            worker_num,
            task: ExecutorTask::None,
        }
    }

    pub fn has_task(&self) -> bool {
        !matches!(&self.task, ExecutorTask::None)
    }

    pub fn get_worker_num(&self) -> usize {
        self.worker_num
    }

    pub fn set_task(&mut self, task: ExecutorTask) {
        self.task = task
    }

    pub unsafe fn execute_task(&mut self, queue: &ExecutorTasksQueue) -> Result<NodeIndex> {
        match std::mem::replace(&mut self.task, ExecutorTask::None) {
            ExecutorTask::None => Err(ErrorCode::LogicalError("Execute none task.")),
            ExecutorTask::Sync(processor) => self.execute_sync_task(processor),
            ExecutorTask::Async(processor) => self.execute_async_task(processor, queue),
            ExecutorTask::AsyncSchedule(boxed_future) => self.schedule_async_task(boxed_future, queue),
        }
    }

    unsafe fn execute_sync_task(&mut self, processor: ProcessorPtr) -> Result<NodeIndex> {
        processor.process()?;
        Ok(processor.id())
    }

    unsafe fn execute_async_task(&mut self, processor: ProcessorPtr, queue: &ExecutorTasksQueue) -> Result<NodeIndex> {
        let id = processor.id();
        let finished = Arc::new(AtomicBool::new(false));
        let mut future = processor.async_process();
        self.schedule_async_task(ExecutingAsyncTask { id, finished, future }, queue)
    }

    unsafe fn schedule_async_task(&mut self, mut task: ExecutingAsyncTask, queue: &ExecutorTasksQueue) -> Result<NodeIndex> {
        task.finished.store(false, Ordering::Relaxed);

        let id = task.id;
        loop {
            let waker = ExecutingAsyncTaskWaker::create(&task.finished);

            let waker = futures::task::waker_ref(&waker);
            let mut cx = Context::from_waker(&waker);

            match task.future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(_)) => { return Ok(id); }
                Poll::Ready(Err(cause)) => { return Err(cause); }
                Poll::Pending => {
                    match queue.push_executing_async_task(self.worker_num, task) {
                        None => { return Ok(id); }
                        Some(t) => { task = t; }
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

