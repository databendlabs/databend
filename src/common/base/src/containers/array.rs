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

// array concat using const generics
pub fn concat<T, const A: usize, const B: usize>(a: &[T; A], b: &[T; B]) -> [T; A + B] {
    let mut result = std::mem::MaybeUninit::uninit();
    let dest = result.as_mut_ptr() as *mut T;
    unsafe {
        std::ptr::copy_nonoverlapping(a.as_ptr(), dest, A);
        std::ptr::copy_nonoverlapping(b.as_ptr(), dest.add(A), B);
        result.assume_init()
    }
}
