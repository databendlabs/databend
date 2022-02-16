// Copyright 2021 Datafuse Labs.
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

use crate::runtime_tracker::ThreadTracker;

pub struct Thread;

impl Thread {
    pub fn named_spawn<F, T>(mut name: Option<String>, f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        let mut thread_builder = Builder::new();

        if let Some(named) = name.take() {
            thread_builder = thread_builder.name(named);
        }

        match ThreadTracker::current_runtime_tracker() {
            None => thread_builder.spawn(f).unwrap(),
            Some(runtime_tracker) => thread_builder
                .spawn(move || {
                    ThreadTracker::create(runtime_tracker);
                    f()
                })
                .unwrap(),
        }
    }

    pub fn spawn<F, T>(f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        Self::named_spawn(None, f)
    }
}
