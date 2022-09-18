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

use std::intrinsics::assume;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ops::DerefMut;

use super::table0::Entry;
use super::traits::Keyable;

pub struct SliceIterPtr<T> {
    slice: *const [T],
    i: usize,
}

impl<T> SliceIterPtr<T> {
    pub fn new(slice: *const [T]) -> Self {
        Self { slice, i: 0 }
    }
}

impl<T> Iterator for SliceIterPtr<T> {
    type Item = *const T;

    fn next(&mut self) -> Option<*const T> {
        if self.i == unsafe { (*self.slice).len() } {
            None
        } else {
            let result = unsafe { &(*self.slice)[self.i] as *const _ };
            self.i += 1;
            Some(result)
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Hashed<K: Keyable> {
    hash: u64,
    key: K,
}

impl<K: Keyable> Hashed<K> {
    #[inline(always)]
    pub fn new(key: K) -> Self {
        Self {
            hash: K::hash(&key),
            key,
        }
    }
    #[inline(always)]
    pub fn key(self) -> K {
        self.key
    }
}

unsafe impl<K: Keyable> Keyable for Hashed<K> {
    #[inline(always)]
    fn is_zero(this: &MaybeUninit<Self>) -> bool {
        unsafe {
            let addr = std::ptr::addr_of!((*this.as_ptr()).key);
            K::is_zero(&*(addr as *const MaybeUninit<K>))
        }
    }

    #[inline(always)]
    fn equals_zero(this: &Self) -> bool {
        K::equals_zero(&this.key)
    }

    #[inline(always)]
    fn hash(&self) -> u64 {
        self.hash
    }
}

pub struct ZeroEntry<K, V>(pub Option<Entry<K, V>>);

impl<K, V> Deref for ZeroEntry<K, V> {
    type Target = Option<Entry<K, V>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> DerefMut for ZeroEntry<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> Drop for ZeroEntry<K, V> {
    fn drop(&mut self) {
        if let Some(e) = self.0.as_mut() {
            unsafe {
                e.val.assume_init_drop();
            }
        }
    }
}

#[inline(always)]
pub unsafe fn read_le(data: *const u8, len: usize) -> u64 {
    assume(0 < len && len <= 8);
    let s = 64 - 8 * len as isize;
    if data as usize & 2048 == 0 {
        (data as *const u64).read_unaligned() & (u64::MAX >> s)
    } else {
        (data.offset(len as isize - 8) as *const u64).read_unaligned() >> s
    }
}
