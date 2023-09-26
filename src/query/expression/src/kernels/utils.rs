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

#[inline(always)]
pub fn store_advance<T>(val: &T, ptr: &mut *mut u8) {
    unsafe {
        std::ptr::copy_nonoverlapping(val as *const T as *const u8, *ptr, std::mem::size_of::<T>());
        *ptr = ptr.add(std::mem::size_of::<T>())
    }
}

#[inline(always)]
pub fn store_advance_aligned<T>(val: T, ptr: &mut *mut T) {
    unsafe {
        std::ptr::write(*ptr, val);
        *ptr = ptr.add(1)
    }
}

#[inline(always)]
pub fn copy_advance_aligned<T>(src: *const T, ptr: &mut *mut T, count: usize) {
    unsafe {
        std::ptr::copy_nonoverlapping(src, *ptr, count);
        *ptr = ptr.add(count);
    }
}

#[inline(always)]
pub fn set_vec_len_by_ptr<T>(vec: &mut Vec<T>, ptr: *const T) {
    unsafe {
        vec.set_len((ptr as usize - vec.as_ptr() as usize) / std::mem::size_of::<T>());
    }
}
