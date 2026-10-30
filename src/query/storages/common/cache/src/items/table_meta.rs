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

use std::mem::size_of;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegment;
use parquet::file::metadata::ParquetMetaData;

use super::size::hash_map;
use super::size::location;
use super::size::option;
use super::size::scalar;
use super::size::statistics;
use super::size::table_schema;
use super::size::vec;
use crate::CachedItem;
use crate::items::size::block_meta;

/// Partitions are opaque `dyn PartInfo`; this is the typical size of a fuse block part.
const PART_INFO_BYTES: usize = 512;

impl CachedItem for TableSnapshot {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>()
            + table_schema(&self.schema)
            + statistics(&self.summary)
            + vec(&self.segments, location)
            + option(&self.cluster_key_meta, |(_, expr)| expr.capacity())
            + option(&self.table_statistics_location, |s| s.capacity())
    }
}

impl CachedItem for TableSnapshotStatistics {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>()
            + hash_map(&self.hll, |_, hll| hll.memory_size())
            + hash_map(&self.top_n, |_, top_n| {
                vec(&top_n.values, |e| scalar(&e.scalar))
            })
            + hash_map(&self.count_min_sketch, |_, cms| cms.memory_size())
            + hash_map(&self.histograms, |_, h| h.memory_size())
    }
}

impl CachedItem for CompactSegmentInfo {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + statistics(&self.summary) + self.raw_block_metas.bytes.capacity()
    }
}

impl CachedItem for ColumnOrientedSegment {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + self.block_metas.memory_size()
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

impl CachedItem for ParquetMetaData {
    fn mem_bytes(&self) -> usize {
        self.memory_size()
    }
}

impl CachedItem for (PartStatistics, Partitions) {
    fn mem_bytes(&self) -> usize {
        let parts = &self.1.partitions;
        size_of::<Self>()
            + parts.capacity() * size_of::<PartInfoPtr>()
            + parts.len() * PART_INFO_BYTES
    }
}
