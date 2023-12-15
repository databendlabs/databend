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

use std::thread::Builder;
use std::thread::JoinHandle;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::runtime::MemStat;
use crate::runtime::ThreadTracker;

pub struct Thread;

pub struct ThreadJoinHandle<T> {
    inner: JoinHandle<T>,
}

impl<T> ThreadJoinHandle<T> {
    pub fn create(inner: JoinHandle<T>) -> ThreadJoinHandle<T> {
        ThreadJoinHandle { inner }
    }

    pub fn join(self) -> Result<T> {
        match self.inner.join() {
            Ok(res) => Ok(res),
            Err(cause) => match cause.downcast_ref::<&'static str>() {
                None => match cause.downcast_ref::<String>() {
                    None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                    Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                },
                Some(message) => Err(ErrorCode::PanicError(message.to_string())),
            },
        }
    }
}

impl Thread {
    pub fn named_spawn<F, T>(mut name: Option<String>, f: F) -> ThreadJoinHandle<T>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        let mut thread_builder = Builder::new();

        #[cfg(debug_assertions)]
        {
            // We need to pass the thread name in the unit test, because the thread name is the test name
            if matches!(std::env::var("UNIT_TEST"), Ok(var_value) if var_value == "TRUE") {
                if let Some(thread_name) = std::thread::current().name() {
                    name = Some(thread_name.to_string());
                }
            }

            thread_builder = thread_builder.stack_size(5 * 1024 * 1024);
        }

        let mut mem_stat_name = String::from("UnnamedThread");

        if let Some(named) = name.take() {
            mem_stat_name = format!("{}Thread", named);
            thread_builder = thread_builder.name(named);
        }

        ThreadJoinHandle::create(match MemStat::current() {
            None => thread_builder.spawn(f).unwrap(),
            Some(memory_tracker) => thread_builder
                .spawn(move || {
                    let c = MemStat::create_child(mem_stat_name, Some(memory_tracker));
                    let s = ThreadTracker::replace_mem_stat(Some(c));
                    debug_assert!(s.is_none(), "a new thread must have no tracker");

                    f()
                })
                .unwrap(),
        })
    }

    pub fn spawn<F, T>(f: F) -> ThreadJoinHandle<T>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        Self::named_spawn(None, f)
    }
}
