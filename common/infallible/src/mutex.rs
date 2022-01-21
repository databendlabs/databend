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

use parking_lot::Mutex as ParkingMutex;
use parking_lot::MutexGuard;

/// A simple wrapper around the lock() function of a std::sync::Mutex
#[derive(Debug)]
pub struct Mutex<T>(ParkingMutex<T>);

unsafe impl<T> Send for Mutex<T> where ParkingMutex<T>: Send {}

unsafe impl<T> Sync for Mutex<T> where ParkingMutex<T>: Sync {}

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
