// Copyright 2020-2022 Jorge C. Leitão
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

mod data;
mod stream;

use databend_common_arrow::arrow::ffi::mmap;

#[test]
fn mmap_slice() {
    let slice = &[1, 2, 3];
    let array = unsafe { mmap::slice(slice) };
    assert_eq!(array.values().as_ref(), &[1, 2, 3]);
    // note: when `slice` is dropped, array must be dropped as-well since by construction of `slice` they share their lifetimes.
}

#[test]
fn mmap_bitmap() {
    let slice = &[123u8, 255];
    let array = unsafe { mmap::bitmap(slice, 2, 14) }.unwrap();
    assert_eq!(array.values_iter().collect::<Vec<_>>(), &[
        false, true, true, true, true, false, true, true, true, true, true, true, true, true
    ]);
    // note: when `slice` is dropped, array must be dropped as-well since by construction of `slice` they share their lifetimes.
}
