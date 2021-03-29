// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Mutex as StdMutex;
use std::sync::MutexGuard;

/// A simple wrapper around the lock() function of a std::sync::Mutex
#[derive(Debug)]
pub struct Mutex<T>(StdMutex<T>);

impl<T> Mutex<T> {
    /// creates mutex
    pub fn new(t: T) -> Self {
        Self(StdMutex::new(t))
    }

    /// lock the mutex
    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.0.lock().expect("Cannot handle a poisoned lock")
    }
}
