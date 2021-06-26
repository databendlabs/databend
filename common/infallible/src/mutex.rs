// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use parking_lot::Mutex as ParkingMutex;
use parking_lot::MutexGuard;

/// A simple wrapper around the lock() function of a std::sync::Mutex
#[derive(Debug)]
pub struct Mutex<T>(ParkingMutex<T>);

impl<T> Mutex<T> {
    /// creates mutex
    pub fn new(t: T) -> Self {
        Self(ParkingMutex::new(t))
    }

    /// lock the mutex
    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.0.lock()
    }
}
