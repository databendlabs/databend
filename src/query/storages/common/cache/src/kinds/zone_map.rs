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

//! Segment zone maps: the footer of each file, and the column chunks read from it. Files are
//! written once, under new locations, so nothing invalidates these entries.

use std::sync::Arc;

use bytes::Bytes;
use parquet::file::metadata::ParquetMetaData;

use crate::CacheDef;
use crate::CachedItem;
use crate::Pool;

/// Footers of segment zone map files, by file location.
pub struct Meta;

impl CacheDef for Meta {
    type Value = Arc<ParquetMetaData>;
    const NAME: &'static str = "memory_cache_segment_zone_map_meta_data";
    const POOL: Pool = Pool::Meta;
    const SHARE_LOADS: bool = true;
}

/// Compressed column chunks, by `{location}#{row_group}#{field}`.
pub struct Chunk;

impl CacheDef for Chunk {
    type Value = SegmentZoneMapChunk;
    const NAME: &'static str = "memory_cache_segment_zone_map_chunk";
    const POOL: Pool = Pool::Index;
    const DISK: bool = true;
}

/// One column chunk as read from the file.
pub struct SegmentZoneMapChunk(Bytes);

impl SegmentZoneMapChunk {
    pub fn new(bytes: Bytes) -> Self {
        SegmentZoneMapChunk(bytes)
    }

    pub fn bytes(&self) -> Bytes {
        self.0.clone()
    }
}

impl CachedItem for SegmentZoneMapChunk {
    fn mem_bytes(&self) -> usize {
        self.0.len()
    }
}
