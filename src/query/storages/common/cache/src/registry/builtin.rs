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

use crate::CacheRegistry;
use crate::kinds::bloom;
use crate::kinds::column;
use crate::kinds::iceberg;
use crate::kinds::inverted;
use crate::kinds::parquet;
use crate::kinds::segment;
use crate::kinds::spatial;
use crate::kinds::table;
use crate::kinds::vector;
use crate::kinds::virtual_column;

impl CacheRegistry {
    /// Every cache kind of a query node, in the order of `system.caches`.
    pub fn builtin() -> CacheRegistry {
        let mut registry = CacheRegistry::default();
        registry
            .register::<table::Snapshot>()
            .register::<table::SnapshotStats>()
            .register::<segment::Info>()
            .register::<segment::ColumnOriented>()
            .register::<segment::BlockMetas>()
            .register::<segment::Stats>()
            .register::<bloom::IndexMeta>()
            .register::<inverted::Meta>()
            .register::<vector::Meta>()
            .register::<spatial::Meta>()
            .register::<virtual_column::Meta>()
            .register::<parquet::Meta>()
            .register::<iceberg::Table>()
            .register::<table::PruneResult>()
            .register::<bloom::Filter>()
            .register::<vector::Index>()
            .register::<vector::Refine>()
            .register::<inverted::Lookup>()
            .register::<inverted::Payload>()
            .register::<inverted::FullPayload>()
            .register::<spatial::Index>()
            .register::<column::Data>()
            .register::<column::Array>();
        registry
    }
}
