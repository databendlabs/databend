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

pub struct UnSafeCellWrap<T: Sized> {
    inner: UnsafeCell<T>,
}

unsafe impl<T: Sized> Send for UnSafeCellWrap<T> {}

unsafe impl<T: Sized> Sync for UnSafeCellWrap<T> {}

impl<T: Sized> UnSafeCellWrap<T> {
    pub fn create(inner: T) -> UnSafeCellWrap<T> {
        UnSafeCellWrap {
            inner: UnsafeCell::new(inner),
        }
    }

    pub fn set_value(&self, value: T) {
        unsafe {
            let inner_value = &mut *self.inner.get();
            *inner_value = value;
        }
    }
}

impl<T: Sized> Deref for UnSafeCellWrap<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.get() }
    }
}
