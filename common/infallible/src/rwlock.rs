// Copyright 2020 Datafuse Labs.
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

use parking_lot::RwLock as ParkingRwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;

/// A simple wrapper around the lock() function of a std::sync::RwLock
/// The only difference is that you don't need to call unwrap() on it.
#[derive(Debug, Default)]
pub struct RwLock<T>(ParkingRwLock<T>);

impl<T> RwLock<T> {
    /// creates a read-write lock
    pub fn new(t: T) -> Self {
        Self(ParkingRwLock::new(t))
    }

    /// lock the rwlock in read mode
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0.read()
    }

    /// lock the rwlock in write mode
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0.write()
    }

    /// return the owned type consuming the lock
    pub fn into_inner(self) -> T {
        self.0.into_inner()
    }
}
