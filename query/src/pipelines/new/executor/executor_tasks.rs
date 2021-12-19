use std::collections::VecDeque;
use std::intrinsics::unreachable;
use std::sync::{Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use common_exception::ErrorCode;
use common_infallible::Mutex;
use common_exception::Result;
use crate::pipelines::new::executor::exector_graph::RunningGraph;

struct WorkersTasks<T> {
    tasks_size: usize,
    workers_tasks: Vec<VecDeque<T>>,
}

impl<T> WorkersTasks<T> {
    pub fn create(workers_size: usize) -> WorkersTasks<T> {
        let mut workers_tasks = Vec::with_capacity(workers_size);

        for _index in 0..workers_size {
            workers_tasks.push(VecDeque::new());
        }

        WorkersTasks { tasks_size: 0, workers_tasks }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks_size == 0
    }

    pub unsafe fn best_worker_id(&mut self, mut priority_works_id: usize) -> usize {
        for _index in 0..self.workers_tasks.len() {
            if !self.workers_tasks[priority_works_id].is_empty() {
                break;
            }

            priority_works_id += 1;
            if priority_works_id >= self.workers_tasks.len() {
                priority_works_id = 0;
            }
        }

        priority_works_id
    }

    pub unsafe fn pop_task(&mut self, worker_id: usize) -> Option<T> {
        self.tasks_size -= 1;
        self.workers_tasks[worker_id].pop_front()
    }
}

pub struct GlobalExecutorTasks<T> {
    workers_tasks: Mutex<WorkersTasks<T>>,
    finished: AtomicBool,
}

impl<T> GlobalExecutorTasks<T> {
    pub fn create(workers_size: usize) -> GlobalExecutorTasks<T> {
        GlobalExecutorTasks {
            finished: AtomicBool::new(false),
            workers_tasks: Mutex::new(WorkersTasks::create(workers_size)),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
    }

    // Pull task from the global task queue
    pub fn steal_task(&self, best_worker_num: usize, thread: &mut ThreadExecutorTasks<T>) {
        unsafe {
            let mut workers_tasks = self.workers_tasks.lock();
            if !workers_tasks.is_empty() {
                let best_worker_id = workers_tasks.best_worker_id(best_worker_num);
                thread.task = workers_tasks.pop_task(best_worker_id);

                // if thread.task.is_some() && !workers_tasks.is_empty() {
                //     // TODO:
                // }

                return;
            }
        }

        self.wait_wakeup(thread)
    }

    pub fn wait_wakeup(&self, thread: &mut ThreadExecutorTasks<T>) {
        // condvar.wait(guard);
    }
}

pub struct ThreadExecutorTasks<T> {
    task: Option<T>,
    async_tasks: VecDeque<T>,
}

impl<T> ThreadExecutorTasks<T> {
    pub fn create(worker_num: usize) -> Self {
        ThreadExecutorTasks::<T> {
            task: None,
            async_tasks: VecDeque::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.task.is_none()
    }

    pub fn execute_task(&self, graph: &RunningGraph) -> Result<()> {
        // TODO: try execute sync or async task.
        if let Some(task) = &self.task {}

        unimplemented!("")
    }

    pub fn wait_wakeup(&self) {
        // condvar.wait(guard);
    }
}

pub type ThreadTasksQueue = ThreadExecutorTasks<usize>;
pub type GlobalTasksQueue = GlobalExecutorTasks<usize>;
