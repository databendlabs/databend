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

//! Segments: compact and column-oriented, the decoded block metas, and segment statistics.

use std::mem::size_of;
use std::sync::Arc;

use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegment;

use crate::CacheDef;
use crate::CachedItem;
use crate::KindLimit;
use crate::Pool;
use crate::size::block_meta;
use crate::size::statistics;
use crate::size::table_schema;
use crate::size::vec;

/// Compact segments, by segment location.
pub struct Info;

impl CacheDef for Info {
    type Value = Arc<CompactSegmentInfo>;
    const NAME: &'static str = "memory_cache_compact_segment_info";
    const POOL: Pool = Pool::Meta;
}

/// Column-oriented segments, by segment location.
pub struct ColumnOriented;

impl CacheDef for ColumnOriented {
    type Value = Arc<ColumnOrientedSegment>;
    const NAME: &'static str = "memory_cache_column_oriented_segment_info";
    const POOL: Pool = Pool::Meta;
}

/// Decoded block metas of a segment, by segment location. Off unless sized, in both backends:
/// decoded metas take several times the memory of the segment.
pub struct BlockMetas;

impl CacheDef for BlockMetas {
    type Value = Arc<Vec<Arc<BlockMeta>>>;
    const NAME: &'static str = "memory_cache_segment_block_metas";
    const POOL: Pool = Pool::Meta;
    const SHARED_LIMIT: KindLimit = KindLimit::DISABLED;
}

/// Segment statistics, by location.
pub struct Stats;

impl CacheDef for Stats {
    type Value = Arc<SegmentStatistics>;
    const NAME: &'static str = "memory_cache_segment_statistics";
    const POOL: Pool = Pool::Meta;
}

impl CachedItem for CompactSegmentInfo {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + statistics(&self.summary) + self.raw_block_metas.bytes.capacity()
    }
}

impl CachedItem for ColumnOrientedSegment {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>()
            + self.block_metas.memory_size()
            + statistics(&self.summary)
            + table_schema(&self.segment_schema)
    }
}

impl CachedItem for Vec<Arc<BlockMeta>> {
    fn mem_bytes(&self) -> usize {
        let arc_inner = size_of::<BlockMeta>() + 2 * size_of::<usize>();
        size_of::<Self>() + vec(self, |b| arc_inner + block_meta(b))
    }
}

impl CachedItem for SegmentStatistics {
    fn mem_bytes(&self) -> usize {
        self.memory_size()
    }
}
