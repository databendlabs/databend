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

//! Cache kinds, by domain. Each file defines its kinds (`CacheDef`) next to their values: how
//! much memory a value takes (`CachedItem`) and, for kinds with a disk tier, how it is stored
//! (`DiskCodec`). Kinds are registered in `CacheRegistry::builtin`.

pub mod bloom;
pub mod column;
pub mod iceberg;
mod index_file;
pub mod inverted;
pub mod parquet;
pub mod segment;
pub mod spatial;
pub mod table;
pub mod vector;
pub mod virtual_column;
