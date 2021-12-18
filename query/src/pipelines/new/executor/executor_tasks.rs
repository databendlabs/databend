use std::collections::VecDeque;
use std::intrinsics::unreachable;
use common_exception::ErrorCode;
use common_infallible::Mutex;

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

        WorkersTasks {
            tasks_size: 0,
            workers_tasks: workers_tasks/*vec![VecDeque::new(); workers_size]*/,
        }
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
}

impl<T> GlobalExecutorTasks<T> {
    pub fn create(workers_size: usize) -> GlobalExecutorTasks<T> {
        GlobalExecutorTasks {
            workers_tasks: Mutex::new(WorkersTasks::create(workers_size)),
        }
    }

    // Pull task from the global task queue
    pub fn fetch_task(&self, priority_works_id: usize, thread: &mut ThreadExecutorTasks<T>) {
        unsafe {
            let mut workers_tasks = self.workers_tasks.lock();

            if !workers_tasks.is_empty() {
                let best_worker_id = workers_tasks.best_worker_id(priority_works_id);
                thread.task = workers_tasks.pop_task(best_worker_id);
            }

            if thread.task.is_some() && !workers_tasks.is_empty() {
                // TODO:
            }
        }
    }
}

pub struct ThreadExecutorTasks<T> {
    task: Option<T>,
    async_tasks: VecDeque<T>,
}
