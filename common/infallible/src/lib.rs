// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod mutex_test;
#[cfg(test)]
mod rwlock_test;

mod mutex;
mod rwlock;

pub use mutex::Mutex;
pub use rwlock::RwLock;
