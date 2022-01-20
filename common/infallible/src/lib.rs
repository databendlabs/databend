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

mod condvar;
mod exit_guard;
mod mutex;
mod rwlock;
mod rwlock_upgrade_read;

pub use condvar::Condvar;
pub use exit_guard::ExitGuard;
pub use mutex::Mutex;
pub use rwlock::RwLock;
pub use rwlock_upgrade_read::RwLockUpgradableReadGuard;

#[macro_export]
macro_rules! exit_scope {
    ($x:block) => {
        use common_infallible::ExitGuard;
        let _exit_guard = ExitGuard::create(move || $x);
    };
}
