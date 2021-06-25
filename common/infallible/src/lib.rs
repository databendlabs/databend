// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod mutex_test;
#[cfg(test)]
mod rwlock_test;

mod exit_guard;
mod mutex;
mod rwlock;

pub use exit_guard::ExitGuard;
pub use mutex::Mutex;
pub use rwlock::RwLock;

#[macro_export]
macro_rules! exit_scope {
    ($x:block) => {
        use common_infallible::ExitGuard;
        let _exit_guard = ExitGuard::create(move || $x);
    };
}

// pub use exit_scope;
