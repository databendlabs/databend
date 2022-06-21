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
use std::ops::Deref;
use std::ops::DerefMut;

use parking_lot::ReentrantMutexGuard as ParkingReentrantMutexGuard;

pub struct ReentrantMutexGuard<'a, T> {
    inner: ParkingReentrantMutexGuard<'a, UnsafeCell<T>>,
}

#[allow(suspicious_auto_trait_impls)]
unsafe impl<'a, T> Send for ReentrantMutexGuard<'a, UnsafeCell<T>> where ParkingReentrantMutexGuard<'a, T>: Send
{}

#[allow(suspicious_auto_trait_impls)]
unsafe impl<'a, T> Sync for ReentrantMutexGuard<'a, UnsafeCell<T>> where ParkingReentrantMutexGuard<'a, T>: Sync
{}

impl<'a, T> ReentrantMutexGuard<'a, T> {
    pub fn create(
        inner: ParkingReentrantMutexGuard<'a, UnsafeCell<T>>,
    ) -> ReentrantMutexGuard<'a, T> {
        ReentrantMutexGuard { inner }
    }
}

impl<'a, T> Deref for ReentrantMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.deref().get() }
    }
}

impl<'a, T> DerefMut for ReentrantMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.inner.deref().get() }
    }
}
