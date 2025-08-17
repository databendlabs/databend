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
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

// Memory storage
mod memory_part;
mod memory_table;

// Null storage
pub mod null;

// Random storage
pub mod random;

// Result cache storage
pub mod result_cache;

// View storage
pub mod view;

// Memory storage exports
pub use memory_table::MemoryTable;
// Null storage exports
pub use null::NullTable;
// Random storage exports
pub use random::{RandomPartInfo, RandomTable};
// Result cache storage exports
pub use result_cache::{
    gen_result_cache_key, gen_result_cache_meta_key, gen_result_cache_prefix,
    ResultCacheMetaManager, ResultCacheReader, ResultScan, WriteResultCacheSink,
};
// View storage exports
pub use view::view_table;
