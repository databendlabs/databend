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
use std::ops::Deref;
use std::ops::DerefMut;

use common_arrow::arrow::bitmap::Bitmap;

pub struct Wrap<T>(pub T);

impl<T> Deref for Wrap<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe fn index_of_unchecked<T>(slice: &[T], item: &T) -> usize {
    (item as *const _ as usize - slice.as_ptr() as usize) / std::mem::size_of::<T>()
}

fn index_of<T>(slice: &[T], item: &T) -> Option<usize> {
    debug_assert!(std::mem::size_of::<T>() > 0);
    let ptr = item as *const T;
    unsafe {
        if slice.as_ptr() < ptr && slice.as_ptr().add(slice.len()) > ptr {
            Some(index_of_unchecked(slice, item))
        } else {
            None
        }
    }
}

/// Just a wrapper structure. Useful for certain impl specializations
/// This is for instance use to implement
/// `impl<T> FromIterator<T::Native> for NoNull<DataArray<T>>`
/// as `Option<T::Native>` was already implemented:
/// `impl<T> FromIterator<Option<T::Native>> for DataArray<T>`
pub struct NoNull<T> {
    inner: T,
}

impl<T> NoNull<T> {
    pub fn new(inner: T) -> Self {
        NoNull { inner }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> Deref for NoNull<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for NoNull<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub fn get_iter_capacity<T, I: Iterator<Item = T>>(iter: &I) -> usize {
    match iter.size_hint() {
        (_lower, Some(upper)) => upper,
        (0, None) => 1024,
        (lower, None) => lower,
    }
}

pub fn combine_validities(lhs: &Option<Bitmap>, rhs: &Option<Bitmap>) -> Option<Bitmap> {
    match (lhs, rhs) {
        (Some(lhs), None) => Some(lhs.clone()),
        (None, Some(rhs)) => Some(rhs.clone()),
        (None, None) => None,
        (Some(lhs), Some(rhs)) => Some(lhs & rhs),
    }
}
