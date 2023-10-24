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

#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

mod common;
mod meta_manager;
mod read;
mod table_function;
mod write;

pub use common::gen_result_cache_key;
pub use common::gen_result_cache_meta_key;
pub use common::gen_result_cache_prefix;
pub use meta_manager::ResultCacheMetaManager;
pub use read::ResultCacheReader;
pub use table_function::ResultScan;
pub use write::WriteResultCacheSink;
