// Copyright 2020 Datafuse Labs.
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

use std::thread::JoinHandle;

use crate::runtime_tracker::ThreadTracker;

pub struct Thread;

impl Thread {
    pub fn spawn<F, T>(f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        let outer_tracker = ThreadTracker::current() as usize;
        std::thread::spawn(move || {
            let outer_tracker = outer_tracker as *const ThreadTracker;

            if !outer_tracker.is_null() {
                // We use the same tracker for std::thread
                ThreadTracker::set_current(outer_tracker);
            }

            f()
        })
    }
}
