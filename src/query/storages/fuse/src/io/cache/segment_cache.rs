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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::Reader;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegment;

pub type BlockMetas = Arc<Vec<Arc<BlockMeta>>>;

/// Segments in both layouts, their decoded block metas, and segment statistics.
/// Values are handed out as `Arc`, like snapshots.
#[derive(Clone)]
pub struct SegmentCache {
    pub(super) segments: MemCache<Arc<CompactSegmentInfo>>,
    pub(super) column_oriented: MemCache<Arc<ColumnOrientedSegment>>,
    pub(super) block_metas: MemCache<BlockMetas>,
    pub(super) stats: MemCache<Arc<SegmentStatistics>>,
}

impl SegmentCache {
    pub async fn get(
        &self,
        reader: &impl Reader<Key = Location, Item = Arc<CompactSegmentInfo>>,
        location: Location,
    ) -> Result<Arc<CompactSegmentInfo>> {
        let value = self.segments.get(reader, location).await?;
        Ok(Arc::clone(&value))
    }

    /// Caches a segment just written, so later reads skip storage.
    pub fn put(&self, location: &str, segment: Arc<CompactSegmentInfo>) {
        self.segments.put(location, segment);
    }

    /// Whether segments can be cached; writers skip building a `CompactSegmentInfo` otherwise.
    pub fn is_enabled(&self) -> bool {
        self.segments.is_enabled()
    }

    /// Block metas of the segment at `location`; `decode` runs only on a miss.
    pub fn block_metas(
        &self,
        location: &str,
        decode: impl FnOnce() -> Result<Vec<Arc<BlockMeta>>>,
    ) -> Result<BlockMetas> {
        let value = self
            .block_metas
            .get_sync(location, || decode().map(Arc::new))?;
        Ok(Arc::clone(&value))
    }

    pub fn lookup_block_metas(&self, location: &str) -> Option<BlockMetas> {
        self.block_metas.lookup(location).map(|v| Arc::clone(&v))
    }

    pub async fn get_stats(
        &self,
        reader: &impl Reader<Key = Location, Item = Arc<SegmentStatistics>>,
        location: Location,
    ) -> Result<Arc<SegmentStatistics>> {
        let value = self.stats.get(reader, location).await?;
        Ok(Arc::clone(&value))
    }

    /// Drops stale statistics after the file at `location` is rewritten.
    pub fn evict_stats(&self, location: &str) {
        self.stats.evict(location);
    }

    pub fn lookup_column_oriented(&self, location: &str) -> Option<Arc<ColumnOrientedSegment>> {
        self.column_oriented
            .lookup(location)
            .map(|v| Arc::clone(&v))
    }

    /// Read-path refill of a column-oriented segment, possibly merged with newly read columns.
    pub fn fill_column_oriented(&self, location: &str, segment: Arc<ColumnOrientedSegment>) {
        self.column_oriented.fill(location, segment);
    }

    pub fn read_only(&self) -> Self {
        SegmentCache {
            segments: self.segments.read_only(),
            column_oriented: self.column_oriented.read_only(),
            block_metas: self.block_metas.read_only(),
            stats: self.stats.read_only(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;

    use databend_common_exception::ErrorCode;
    use databend_common_expression::DataBlock;
    use databend_common_expression::TableSchema;
    use databend_storages_common_cache::CacheKind;
    use databend_storages_common_cache::CacheManager;
    use databend_storages_common_cache::kinds::segment;
    use databend_storages_common_cache::legacy::LegacyCapacity;
    use databend_storages_common_cache::legacy::LegacyStore;
    use databend_storages_common_table_meta::meta::Compression;
    use databend_storages_common_table_meta::meta::SegmentInfo;
    use databend_storages_common_table_meta::meta::Statistics;
    use databend_storages_common_table_meta::meta::Versioned;
    use opendal::Operator;
    use opendal::services::Memory;

    use super::*;
    use crate::io::FuseCaches;
    use crate::io::SegmentInfoReader;
    use crate::io::SegmentStatsReader;

    fn caches() -> Arc<FuseCaches> {
        let store = Arc::new(LegacyStore::new([
            (
                CacheKind::of::<segment::Info>(),
                LegacyCapacity::Bytes(1 << 20),
            ),
            (
                CacheKind::of::<segment::ColumnOriented>(),
                LegacyCapacity::Bytes(1 << 20),
            ),
            (
                CacheKind::of::<segment::BlockMetas>(),
                LegacyCapacity::Items(10),
            ),
            (
                CacheKind::of::<segment::Stats>(),
                LegacyCapacity::Bytes(1 << 20),
            ),
        ]));
        FuseCaches::new(&CacheManager::new(store, HashMap::new()))
    }

    fn block(i: u64) -> Arc<BlockMeta> {
        Arc::new(BlockMeta::new(
            i,
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            None,
            (format!("1/2/_b/{i}.parquet"), 0),
            None,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Compression::Lz4,
            None,
        ))
    }

    async fn write_segment(operator: &Operator, path: &str, blocks: u64) -> Result<Location> {
        let segment = SegmentInfo::new((0..blocks).map(block).collect(), Statistics::default());
        operator.write(path, segment.to_bytes()?).await?;
        Ok((path.to_string(), SegmentInfo::VERSION))
    }

    #[tokio::test]
    async fn test_segment_read_through_cache() -> Result<()> {
        let operator = Operator::new(Memory::default())?.finish();
        let reader = SegmentInfoReader::new(operator.clone(), Arc::new(TableSchema::empty()));
        let segments = &caches().segments;

        let cached = write_segment(&operator, "1/2/_sg/a.mpk", 3).await?;
        assert_eq!(
            segments
                .get(&reader, cached.clone())
                .await?
                .block_metas()?
                .len(),
            3
        );

        // A read-only miss is served but not cached.
        let uncached = write_segment(&operator, "1/2/_sg/b.mpk", 1).await?;
        segments.read_only().get(&reader, uncached.clone()).await?;

        operator.delete(&cached.0).await?;
        operator.delete(&uncached.0).await?;
        segments.get(&reader, cached).await?;
        let missing = segments.get(&reader, uncached).await;
        assert_eq!(
            missing.err().map(|e| e.code()),
            Some(ErrorCode::STORAGE_NOT_FOUND)
        );
        Ok(())
    }

    #[test]
    fn test_block_metas_decoded_once() -> Result<()> {
        let segments = &caches().segments;
        let decodes = Cell::new(0);
        let decode = || {
            decodes.set(decodes.get() + 1);
            Ok(vec![block(0), block(1)])
        };
        assert_eq!(segments.block_metas("a", decode)?.len(), 2);
        assert_eq!(segments.block_metas("a", decode)?.len(), 2);
        assert_eq!(decodes.get(), 1);
        assert_eq!(segments.lookup_block_metas("a").unwrap().len(), 2);

        // Read-only views decode on every miss and leave nothing behind.
        segments.read_only().block_metas("b", decode)?;
        assert_eq!(decodes.get(), 2);
        assert!(segments.lookup_block_metas("b").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_segment_stats_and_evict() -> Result<()> {
        let operator = Operator::new(Memory::default())?.finish();
        let reader = SegmentStatsReader::new(operator.clone());
        let segments = &caches().segments;
        let location = ("1/2/_ss/a.mpk".to_string(), SegmentStatistics::VERSION);
        let stats = SegmentStatistics::new(vec![], vec![]);
        operator.write(&location.0, stats.to_bytes()?).await?;

        segments.get_stats(&reader, location.clone()).await?;
        operator.delete(&location.0).await?;
        segments.get_stats(&reader, location.clone()).await?;

        // A rewritten statistics file must be read again.
        segments.evict_stats(&location.0);
        let missing = segments.get_stats(&reader, location).await.unwrap_err();
        assert_eq!(missing.code(), ErrorCode::STORAGE_NOT_FOUND);
        Ok(())
    }

    #[test]
    fn test_column_oriented_fill_respects_read_only() {
        let segments = &caches().segments;
        let segment = || {
            Arc::new(ColumnOrientedSegment {
                block_metas: DataBlock::empty(),
                summary: Statistics::default(),
                segment_schema: TableSchema::empty(),
            })
        };
        segments.read_only().fill_column_oriented("a", segment());
        assert!(segments.lookup_column_oriented("a").is_none());
        segments.fill_column_oriented("a", segment());
        assert!(segments.lookup_column_oriented("a").is_some());
    }
}
