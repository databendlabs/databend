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
    // ANALYZE rewrites this location and puts the new segment under the same key.
    // A miss must not join a load started before that write.
    const DISK: bool = true;
}

/// Column-oriented segments, by segment location.
pub struct ColumnOriented;

impl CacheDef for ColumnOriented {
    type Value = Arc<ColumnOrientedSegment>;
    const NAME: &'static str = "memory_cache_column_oriented_segment_info";
    const POOL: Pool = Pool::Meta;
    const DISK: bool = true;
}

/// Decoded block metas of a segment, by segment location: saves decoding the raw block metas
/// of a `CompactSegmentInfo` again when the same segment is pruned, replaced into or fetched
/// from soon after.
///
/// Off by default in both backends: decoded metas take several times the memory of the
/// segment (about ten times for a wide table), and a scan over more segments than fit misses
/// on every one while turning the meta pool over, evicting the segments it shares the pool
/// with and taking memory from the page cache that serves the disk cache. Turn it on with a
/// byte limit (`cache.kind_max_bytes`, `set_cache_capacity`) for workloads that revisit a set
/// of segments small enough to keep decoded. The legacy backend sizes it by
/// `segment_block_metas_count`, 0 by default, as on `main`.
pub struct BlockMetas;

impl CacheDef for BlockMetas {
    type Value = Arc<Vec<Arc<BlockMeta>>>;
    const NAME: &'static str = "memory_cache_segment_block_metas";
    const POOL: Pool = Pool::Meta;
    const SHARED_DEFAULT: KindLimit = KindLimit::DISABLED;
}

/// Segment statistics, by location. Loads are not shared: `ANALYZE` rewrites the file in
/// place and evicts it. Not on disk: only writes and maintenance read them, and the disk cache
/// serves queries.
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

    fn legacy_bytes(&self) -> usize {
        size_of::<Self>() + self.raw_block_metas.bytes.len()
    }
}

impl CachedItem for ColumnOrientedSegment {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>()
            + self.block_metas.memory_size()
            + statistics(&self.summary)
            + table_schema(&self.segment_schema)
    }

    fn legacy_bytes(&self) -> usize {
        size_of::<Self>() + self.block_metas.memory_size()
    }
}

impl CachedItem for Vec<Arc<BlockMeta>> {
    fn mem_bytes(&self) -> usize {
        let arc_inner = size_of::<BlockMeta>() + 2 * size_of::<usize>();
        size_of::<Self>() + vec(self, |b| arc_inner + block_meta(b))
    }

    fn legacy_bytes(&self) -> usize {
        0
    }
}

impl CachedItem for SegmentStatistics {
    fn mem_bytes(&self) -> usize {
        self.memory_size()
    }
}
