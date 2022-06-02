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

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Estimation for heapsize calculation. Usable to replace call to allocator method (for some
//! allocators or simply because we just need a deterministic cunsumption measurement).

use std::mem::size_of;
use std::mem::size_of_val;
use std::sync::Arc;

use super::malloc_size::MallocShallowSizeOf;
use super::malloc_size::MallocSizeOf;
use super::malloc_size::MallocSizeOfOps;
use super::malloc_size::MallocUnconditionalShallowSizeOf;

impl<T: ?Sized> MallocShallowSizeOf for Box<T> {
    fn shallow_size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        size_of_val(&**self)
    }
}

impl MallocSizeOf for String {
    fn size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        self.capacity() * size_of::<u8>()
    }
}

impl<T> MallocShallowSizeOf for Vec<T> {
    fn shallow_size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        self.capacity() * size_of::<T>()
    }
}

impl<T> MallocUnconditionalShallowSizeOf for Arc<T> {
    fn unconditional_shallow_size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        size_of::<T>()
    }
}
