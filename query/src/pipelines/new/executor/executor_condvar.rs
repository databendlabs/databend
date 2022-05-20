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

use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_base::infallible::{Condvar};
use common_base::infallible::Mutex;


struct WorkerCondvar {
    mutex: Mutex<bool>,
    condvar: Condvar,
}

impl WorkerCondvar {
    pub fn create() -> WorkerCondvar {
        WorkerCondvar {
            mutex: Mutex::new(false),
            condvar: Condvar::create(),
        }
    }
}

pub struct WorkersCondvar {
    waiting_async_task: AtomicUsize,
    workers_condvar: Vec<WorkerCondvar>,
}

impl WorkersCondvar {
    pub fn create(workers: usize) -> Arc<WorkersCondvar> {
        let mut workers_condvar = Vec::with_capacity(workers);

        for _index in 0..workers {
            workers_condvar.push(WorkerCondvar::create());
        }

        Arc::new(WorkersCondvar {
            workers_condvar,
            waiting_async_task: AtomicUsize::new(0),
        })
    }

    pub fn has_waiting_async_task(&self) -> bool {
        self.waiting_async_task.load(Ordering::Relaxed) != 0
    }

    pub fn inc_active_async_worker(&self) {
        self.waiting_async_task.fetch_add(1, Ordering::Release);
    }

    pub fn dec_active_async_worker(&self) {
        self.waiting_async_task.fetch_sub(1, Ordering::Release);
    }

    pub fn wakeup(&self, worker_id: usize) {
        let mut wait_guard = self.workers_condvar[worker_id].mutex.lock();
        *wait_guard = true;
        self.workers_condvar[worker_id].condvar.notify_one();
    }

    pub fn wait(&self, worker_id: usize, finished: Arc<AtomicBool>) {
        let mut wait_guard = self.workers_condvar[worker_id].mutex.lock();

        while !*wait_guard && !finished.load(Ordering::Relaxed) {
            self.workers_condvar[worker_id].condvar.wait(&mut wait_guard);
        }

        *wait_guard = false;
    }
}


// Inspired by ClickHouse
// All operations (except init) are O(1). especially wake any.
pub struct WorkersWaitingStatus {
    stack: Vec<usize>,
    stack_size: usize,
    worker_pos_in_stack: Vec<usize>,
}

impl WorkersWaitingStatus {
    pub fn create(size: usize) -> WorkersWaitingStatus {
        let stack = (0..size).collect::<Vec<_>>();
        let worker_pos_in_stack = (0..size).collect::<Vec<_>>();
        WorkersWaitingStatus { stack, worker_pos_in_stack, stack_size: 0 }
    }

    pub fn is_last_active_worker(&self) -> bool {
        self.waiting_size() + 1 >= self.worker_pos_in_stack.len()
    }

    pub fn waiting_size(&self) -> usize {
        self.stack_size
    }

    pub fn is_waiting(&self, thread: usize) -> bool {
        self.worker_pos_in_stack[thread] < self.stack_size
    }

    pub fn wait_worker(&mut self, thread: usize) {
        assert!(!self.is_waiting(thread));
        self.swap_worker_status(thread, self.stack[self.stack_size]);
        self.stack_size += 1;
    }

    pub fn wakeup_worker(&mut self, thread: usize) -> usize {
        assert!(self.is_waiting(thread));

        self.stack_size -= 1;
        let thread_num = self.stack[self.stack_size];
        self.swap_worker_status(thread, thread_num);
        thread_num
    }

    pub fn wakeup_any_worker(&mut self) -> usize {
        assert_ne!(self.stack_size, 0);

        self.stack_size -= 1;
        self.stack[self.stack_size]
    }

    fn swap_worker_status(&mut self, first: usize, second: usize) {
        self.worker_pos_in_stack.swap(first, second);
        self.stack.swap(self.worker_pos_in_stack[first], self.worker_pos_in_stack[second]);
    }
}
