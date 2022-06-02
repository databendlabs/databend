// Copyright 2022 Datafuse Labs.
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

use std::cell::UnsafeCell;
use parking_lot::ReentrantMutex as ParkingReentrantMutex;
use crate::infallible::ReentrantMutexGuard;

/// A simple wrapper around the lock() function of a ReentrantMutex
#[derive(Debug)]
pub struct ReentrantMutex<T>(ParkingReentrantMutex<UnsafeCell<T>>);

unsafe impl<T> Send for ReentrantMutex<UnsafeCell<T>> where ParkingReentrantMutex<T>: Send {}

unsafe impl<T> Sync for ReentrantMutex<UnsafeCell<T>> where ParkingReentrantMutex<T>: Sync {}

impl<T> ReentrantMutex<T> {
    /// creates mutex
    pub fn new(t: T) -> Self {
        Self(ParkingReentrantMutex::new(UnsafeCell::new(t)))
    }

    /// lock the mutex
    pub fn lock(&self) -> ReentrantMutexGuard<'_, T> {
        ReentrantMutexGuard::create(self.0.lock())
    }
}


