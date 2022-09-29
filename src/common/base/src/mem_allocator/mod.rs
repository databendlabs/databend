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

mod global_allocator;
mod je_allocator;
mod mmap_allocator;
mod system_allocator;

pub use global_allocator::GlobalAllocator;
pub use je_allocator::JEAllocator;
pub use mmap_allocator::MmapAllocator;
pub use system_allocator::SystemAllocator;

#[cfg(feature = "memory-profiling")]
mod profiling;

#[cfg(feature = "memory-profiling")]
pub use profiling::dump_profile;
