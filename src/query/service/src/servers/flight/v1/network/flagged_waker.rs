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

//! FlaggedWaker: wraps an executor waker with an AtomicBool flag.
//!
//! Used by synchronous pipeline processors (ThreadChannelReader, ThreadChannelWriter)
//! to poll async futures without `async_process`. When the inner waker fires,
//! the flag is set so the processor's `event()` knows to re-poll the future.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Wake;
use std::task::Waker;

/// Wraps an executor waker to also set an AtomicBool flag when woken.
///
/// This allows a processor's `event()` to detect that an async operation
/// has completed without needing to poll the future.
pub struct FlaggedWaker {
    inner: Waker,
    flag: Arc<AtomicBool>,
}

impl FlaggedWaker {
    pub fn create(executor_waker: Waker) -> Waker {
        let flag = Arc::new(AtomicBool::new(false));
        Waker::from(Arc::new(Self {
            inner: executor_waker,
            flag,
        }))
    }
}

impl Wake for FlaggedWaker {
    fn wake(self: Arc<Self>) {
        self.flag.store(true, Ordering::Release);
        self.inner.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.flag.store(true, Ordering::Release);
        self.inner.wake_by_ref();
    }
}
