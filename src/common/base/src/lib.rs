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

#![allow(clippy::uninlined_format_args)]
#![allow(incomplete_features)]
#![feature(allocator_api)]
#![feature(thread_local)]
#![feature(ptr_metadata)]
#![feature(result_flattening)]
#![feature(try_trait_v2)]
#![feature(thread_id_value)]
#![feature(backtrace_frames)]
#![feature(alloc_error_hook)]
#![feature(slice_swap_unchecked)]
#![feature(variant_count)]
#![feature(ptr_alignment_type)]
#![feature(vec_into_raw_parts)]
#![feature(slice_ptr_get)]
#![feature(alloc_layout_extra)]
#![feature(let_chains)]

pub mod base;
pub mod containers;
pub mod display;
pub mod future;
pub mod headers;
pub mod http_client;
pub mod mem_allocator;
pub mod obfuscator;
pub mod rangemap;
pub mod runtime;
pub mod vec_ext;
pub mod version;

pub use runtime::dump_backtrace;
pub use runtime::get_all_tasks;
pub use runtime::set_alloc_error_hook;
pub use runtime::AsyncTaskItem;
pub use runtime::JoinHandle;
pub use runtime::GLOBAL_TASK;
