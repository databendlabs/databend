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

pub trait VecExt<T> {
    /// Remove the first element that is equal to the given item.
    fn remove_first(&mut self, item: &T) -> Option<T>
    where T: PartialEq;

    /// Will push an item and not check if there is enough capacity
    ///
    /// # Safety
    /// Caller must ensure the array has enough capacity to hold `T`.
    unsafe fn push_unchecked(&mut self, value: T);

    /// # Safety
    /// Caller must ensure the array has enough capacity to hold `&[T]`.
    unsafe fn extend_from_slice_unchecked(&mut self, val: &[T]);
}

impl<T> VecExt<T> for Vec<T> {
    fn remove_first(&mut self, item: &T) -> Option<T>
    where T: PartialEq {
        let pos = self.iter().position(|x| x == item)?;
        Some(self.remove(pos))
    }

    #[inline]
    unsafe fn push_unchecked(&mut self, value: T) {
        debug_assert!(self.capacity() > self.len());
        let end = unsafe { self.as_mut_ptr().add(self.len()) };
        unsafe { std::ptr::write(end, value) };
        unsafe { self.set_len(self.len() + 1) };
    }

    unsafe fn extend_from_slice_unchecked(&mut self, val: &[T]) {
        debug_assert!(self.capacity() >= self.len() + val.len());
        let end = unsafe { self.as_mut_ptr().add(self.len()) };
        unsafe { std::ptr::copy_nonoverlapping(val.as_ptr(), end, val.len()) };
        unsafe { self.set_len(self.len() + val.len()) };
    }
}

pub trait VecU8Ext {
    /// # Safety
    /// Caller must ensure the array has enough capacity to hold `V`.
    unsafe fn store_value_uncheckd<V>(&mut self, val: &V);
}

impl VecU8Ext for Vec<u8> {
    unsafe fn store_value_uncheckd<T>(&mut self, val: &T) {
        debug_assert!(self.capacity() >= self.len() + std::mem::size_of::<T>());
        let end = unsafe { self.as_mut_ptr().add(self.len()) };
        unsafe {
            std::ptr::copy_nonoverlapping(
                val as *const T as *const u8,
                end,
                std::mem::size_of::<T>(),
            )
        };
        unsafe { self.set_len(self.len() + std::mem::size_of::<T>()) };
    }
}
