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

use std::cell::SyncUnsafeCell;
use std::ops::Deref;

/// A C-style cell that provides interior mutability without runtime borrow checking.
///
/// This is a thin wrapper around `SyncUnsafeCell` that allows shared mutable access
/// to the inner value without the overhead of `RefCell` or `Mutex`. It's designed
/// for performance-critical scenarios where the caller can guarantee memory safety.
///
/// # Safety
///
/// - The caller must ensure that there are no data races when accessing the inner value
/// - Multiple mutable references to the same data must not exist simultaneously
/// - This should only be used when you can statically guarantee exclusive access
///   or when protected by external synchronization mechanisms
///
/// # Use Cases
///
/// - High-performance hash join operations where contention is managed externally
/// - Single-threaded contexts where `RefCell`'s runtime checks are unnecessary overhead
/// - Data structures that implement their own synchronization protocols
pub struct CStyleCell<T: ?Sized> {
    inner: SyncUnsafeCell<T>,
}

impl<T> CStyleCell<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: SyncUnsafeCell::new(inner),
        }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }

    /// Returns a mutable reference to the inner value.
    ///
    /// # Safety
    ///
    /// The caller must ensure that no other references (mutable or immutable)
    /// to the inner value exist when this method is called, and that the
    /// returned reference is not used concurrently with other accesses.
    #[allow(clippy::mut_from_ref)]
    pub fn as_mut(&self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}

impl<T: ?Sized> Deref for CStyleCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.get() }
    }
}
