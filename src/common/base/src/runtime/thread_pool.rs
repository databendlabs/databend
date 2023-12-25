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

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_exception::Result;

use crate::runtime::Thread;

// Simple thread pool implementation,
// which can run more tasks on limited threads and wait for all tasks to finished.
pub struct ThreadPool {
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

pub struct TaskJoinHandler<T: Send> {
    rx: Receiver<T>,
}

impl<T: Send> TaskJoinHandler<T> {
    pub fn create(rx: Receiver<T>) -> TaskJoinHandler<T> {
        TaskJoinHandler::<T> { rx }
    }

    pub fn join(&self) -> T {
        self.rx.recv_blocking().unwrap()
    }
}

impl ThreadPool {
    pub fn create(threads: usize) -> Result<ThreadPool> {
        let (tx, rx) = async_channel::unbounded::<Box<dyn FnOnce() + Send + 'static>>();

        for _index in 0..threads {
            let thread_rx = rx.clone();
            Thread::spawn(move || {
                while let Ok(task) = thread_rx.recv_blocking() {
                    task();
                }
            });
        }

        Ok(ThreadPool { tx })
    }

    pub fn execute<F, R>(&self, f: F) -> TaskJoinHandler<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = async_channel::bounded(1);
        let _ = self.tx.send_blocking(Box::new(move || {
            let _ = tx.send_blocking(f());
        }));

        TaskJoinHandler::create(rx)
    }
}
