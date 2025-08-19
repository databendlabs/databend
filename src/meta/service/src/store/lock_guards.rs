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

use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;
use std::time::Instant;

use log::info;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;

#[derive(Debug)]
pub struct WriteGuard<'a, T: ?Sized> {
    acquired: Instant,
    purpose: String,
    inner: RwLockWriteGuard<'a, T>,
}

impl<'a, T> WriteGuard<'a, T> {
    pub fn new(purpose: impl ToString, inner: RwLockWriteGuard<'a, T>) -> Self {
        Self {
            acquired: Instant::now(),
            purpose: purpose.to_string(),
            inner,
        }
    }
}

impl<T> Deref for WriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<T> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.deref_mut()
    }
}

impl<T> fmt::Display for WriteGuard<'_, T>
where T: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StateMachineLock-Write-Guard: {} {}",
            self.inner, self.purpose,
        )
    }
}

impl<T: ?Sized> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        let elapsed = self.acquired.elapsed();
        info!(
            "StateMachineLock-Write-Release: total: {:?}; {}",
            elapsed, self.purpose,
        );
    }
}

#[derive(Debug)]
pub struct ReadGuard<'a, T: ?Sized> {
    acquired: Instant,
    purpose: String,
    inner: RwLockReadGuard<'a, T>,
}

impl<'a, T> ReadGuard<'a, T> {
    pub fn new(purpose: impl ToString, inner: RwLockReadGuard<'a, T>) -> Self {
        Self {
            acquired: Instant::now(),
            purpose: purpose.to_string(),
            inner,
        }
    }
}

impl<T> Deref for ReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<T> fmt::Display for ReadGuard<'_, T>
where T: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StateMachineLock-Read-Guard: {} {}",
            self.inner, self.purpose
        )
    }
}

impl<T: ?Sized> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        let elapsed = self.acquired.elapsed();
        info!(
            "StateMachineLock-Read-Release: total: {:?}; {}",
            elapsed, self.purpose,
        );
    }
}
