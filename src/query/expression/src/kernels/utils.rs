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

/// # Safety
/// # As: core::ptr::copy_nonoverlapping
#[inline]
pub unsafe fn store<T: Copy>(val: &T, ptr: *mut u8) {
    core::ptr::copy_nonoverlapping(val as *const T as *const u8, ptr, std::mem::size_of::<T>());
}

/// # Safety
/// # As: core::ptr::read_unaligned
#[inline]
pub unsafe fn read<T>(ptr: *const u8) -> T {
    core::ptr::read_unaligned::<T>(ptr as _)
}
