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

mod default;
mod global;
#[cfg(feature = "jemalloc")]
mod jemalloc;
#[cfg(feature = "mimalloc")]
mod mimalloc;
mod mmap;
#[cfg(feature = "memory-profiling")]
mod profiling;
mod std_;

pub use default::DefaultAllocator;
pub use global::GlobalAllocator;
#[cfg(feature = "jemalloc")]
pub use jemalloc::JEAllocator;
#[cfg(feature = "mimalloc")]
pub use mimalloc::MIAllocator;
pub use mmap::MmapAllocator;
#[cfg(feature = "memory-profiling")]
pub use profiling::dump_profile;
pub use std_::StdAllocator;
