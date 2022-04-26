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

mod allocators;
mod malloc_size;
pub use allocators::new_malloc_size_ops;
pub use allocators::Allocator;
pub use allocators::MallocSizeOfExt;
pub use malloc_size::MallocShallowSizeOf;
pub use malloc_size::MallocSizeOf;
pub use malloc_size::MallocSizeOfOps;

/// Heap size of structure.
///
/// Structure can be anything that implements MallocSizeOf.
pub fn malloc_size<T: MallocSizeOf + ?Sized>(t: &T) -> usize {
    MallocSizeOf::size_of(t, &mut allocators::new_malloc_size_ops())
}

#[cfg(feature = "memory-profiling")]
mod profiling;
#[cfg(feature = "memory-profiling")]
pub use profiling::dump_profile;
