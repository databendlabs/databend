//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashMap;

use crate::storages::fuse::meta::ColumnId;
use crate::storages::index::ColumnStatistics;

/// Meta information of a block (currently, the parquet file)
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BlockMeta {
    /// Pointer of the data Block
    pub row_count: u64,
    pub block_size: u64,
    pub file_size: u64,
    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub location: BlockLocation,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BlockLocation {
    pub path: String,
    // for parquet, this filed can be used to fetch the meta data without seeking around
    pub meta_size: u64,
}
