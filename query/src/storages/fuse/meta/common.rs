//  Copyright 2022 Datafuse Labs.
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

use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::storages::index::ClusterStatistics;
use crate::storages::index::ColumnStatistics;

pub type ColumnId = u32;
pub type FormatVersion = u64;
pub type SnapshotId = Uuid;
pub type Location = (String, FormatVersion);

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Statistics {
    pub row_count: u64,
    pub block_count: u64,

    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,

    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub cluster_stats: Option<ClusterStatistics>,
}

/// Thing has a u64 version nubmer
pub trait Versioned<const V: u64>
where Self: Sized
{
    const VERSION: u64 = V;
}

#[derive(Serialize, Deserialize, PartialEq, Copy, Clone, Debug)]
pub enum Compression {
    Lz4,
    Lz4Raw,
}

impl Compression {
    pub fn legacy() -> Self {
        Compression::Lz4
    }
}
