// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::RwLock as StdRwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

/// A simple wrapper around the lock() function of a std::sync::RwLock
/// The only difference is that you don't need to call unwrap() on it.
#[derive(Debug, Default)]
pub struct RwLock<T>(StdRwLock<T>);

impl<T> RwLock<T> {
    /// creates a read-write lock
    pub fn new(t: T) -> Self {
        Self(StdRwLock::new(t))
    }

    /// lock the rwlock in read mode
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0.read().expect("Cannot handle a poisoned lock")
    }

    /// lock the rwlock in write mode
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0.write().expect("Cannot handle a poisoned lock")
    }

    /// return the owned type consuming the lock
    pub fn into_inner(self) -> T {
        self.0.into_inner().expect("Cannot handle a poisoned lock")
    }
}
