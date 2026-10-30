// Copyright 2026 Datafuse Labs
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

//! Footers of parquet files read by the parquet and stage tables.

use std::sync::Arc;

use parquet::file::metadata::ParquetMetaData;

use crate::CacheDef;
use crate::CachedItem;
use crate::Pool;

/// Parquet metadata, by file location.
pub struct Meta;

impl CacheDef for Meta {
    type Value = Arc<ParquetMetaData>;
    const NAME: &'static str = "memory_cache_parquet_meta_data";
    const POOL: Pool = Pool::Meta;
}

impl CachedItem for ParquetMetaData {
    fn mem_bytes(&self) -> usize {
        self.memory_size()
    }
}
