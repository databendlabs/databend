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

use std::sync::Arc;

use databend_common_base::runtime::drop_guard;
use parking_lot::Mutex;

pub trait UnlockApi: Sync + Send {
    fn unlock(&self, revision: u64);
}

pub struct LockGuard {
    lock_mgr: Arc<dyn UnlockApi>,
    revision: u64,
}

impl LockGuard {
    pub fn new(lock_mgr: Arc<dyn UnlockApi>, revision: u64) -> Self {
        LockGuard { lock_mgr, revision }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        drop_guard(move || {
            self.lock_mgr.unlock(self.revision);
        })
    }
}

#[derive(Clone)]
pub struct SharedLockGuard {
    inner: Arc<Mutex<Option<Arc<LockGuard>>>>,
}

impl SharedLockGuard {
    pub fn new(guard: Arc<LockGuard>) -> Self {
        SharedLockGuard {
            inner: Arc::new(Mutex::new(Some(guard))),
        }
    }

    pub fn try_take(&self) -> Option<Arc<LockGuard>> {
        self.inner.lock().take()
    }
}
