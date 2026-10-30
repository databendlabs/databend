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

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CachedVectorIndex;
use databend_storages_common_cache::ColumnData;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::SizedColumnArray;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::inverted;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::spatial;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::kinds::vector;
use databend_storages_common_cache::kinds::virtual_column;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::InvertedIndexLookupBytes;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_index::InvertedIndexPayloadBytes;
use databend_storages_common_index::SpatialIndexFile;
use databend_storages_common_index::SpatialIndexMeta;
use databend_storages_common_index::VectorIndexFile;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_index::filters::FilterImpl;

use crate::io::cache::SegmentCache;
use crate::io::cache::SnapshotCache;

/// Process-wide caches of fuse tables. Every kind is bound to its value type here.
pub struct FuseCaches {
    pub snapshots: SnapshotCache,
    pub segments: SegmentCache,
    pub index_metas: IndexMetaCache,
    /// One entry per bloom index column, keyed by `{index_path}-{filter_name}`.
    pub bloom_filters: MemCache<Arc<FilterImpl>>,
    /// Spatial index columns, keyed by `{index_path}-{column}`.
    pub spatial_files: MemCache<SpatialIndexFile>,
    /// FP16 refine pages of vector indexes, keyed by `{index_path}:{page}`.
    pub vector_refine_pages: MemCache<VectorIndexFile>,
    /// Pruning results of deterministic scans (`enable_prune_cache`).
    pub prune_results: MemCache<(PartStatistics, Partitions)>,
    pub inverted: InvertedIndexCache,
    /// Decoded HNSW indexes. Readers holding one keep it charged.
    pub vector_indexes: MemCache<CachedVectorIndex>,
    /// Raw column bytes, keyed by `TableDataCacheKey` without the type.
    pub column_data: MemCache<ColumnData>,
    /// Deserialized columns, keyed by `TableDataCacheKey` with the type.
    pub column_arrays: MemCache<SizedColumnArray>,
}

/// Inverted index bundles: the footer, then whole lookup files or 64 KiB pages.
#[derive(Clone)]
pub struct InvertedIndexCache {
    /// Persisted footers, keyed by `inverted_index_meta_cache_key`.
    pub meta: MemCache<InvertedIndexMeta>,
    /// Term dictionaries and small fieldnorm / fast files, whole or paged.
    pub lookup: MemCache<InvertedIndexLookupBytes>,
    /// Pages of postings, positions, store and large fieldnorm / fast files.
    pub payload: MemCache<InvertedIndexPayloadBytes>,
    /// Complete `.idx` / `.pos` components, apart from pages so warmups cannot evict hot pages.
    pub full_payload: MemCache<InvertedIndexPayloadBytes>,
}

impl InvertedIndexCache {
    pub fn read_only(&self) -> Self {
        InvertedIndexCache {
            meta: self.meta.read_only(),
            lookup: self.lookup.read_only(),
            payload: self.payload.read_only(),
            full_payload: self.full_payload.read_only(),
        }
    }
}

/// Footers of index files, keyed by index file path. Entries are `Arc`, like snapshots.
#[derive(Clone)]
pub struct IndexMetaCache {
    pub bloom: MemCache<Arc<BloomIndexMeta>>,
    pub vector: MemCache<Arc<VectorIndexMeta>>,
    pub spatial: MemCache<Arc<SpatialIndexMeta>>,
    pub virtual_column: MemCache<Arc<VirtualColumnFileMeta>>,
}

impl IndexMetaCache {
    pub fn read_only(&self) -> Self {
        IndexMetaCache {
            bloom: self.bloom.read_only(),
            vector: self.vector.read_only(),
            spatial: self.spatial.read_only(),
            virtual_column: self.virtual_column.read_only(),
        }
    }
}

impl FuseCaches {
    pub fn new(manager: &CacheManager) -> Arc<Self> {
        Arc::new(FuseCaches {
            snapshots: SnapshotCache {
                snapshots: manager.cache::<table::Snapshot>(),
                stats: manager.cache::<table::SnapshotStats>(),
            },
            segments: SegmentCache {
                segments: manager.cache::<segment::Info>(),
                column_oriented: manager.cache::<segment::ColumnOriented>(),
                block_metas: manager.cache::<segment::BlockMetas>(),
                stats: manager.cache::<segment::Stats>(),
            },
            index_metas: IndexMetaCache {
                bloom: manager.cache::<bloom::IndexMeta>(),
                vector: manager.cache::<vector::Meta>(),
                spatial: manager.cache::<spatial::Meta>(),
                virtual_column: manager.cache::<virtual_column::Meta>(),
            },
            bloom_filters: manager.cache::<bloom::Filter>(),
            spatial_files: manager.cache::<spatial::Index>(),
            vector_refine_pages: manager.cache::<vector::Refine>(),
            prune_results: manager.cache::<table::PruneResult>(),
            inverted: InvertedIndexCache {
                meta: manager.cache::<inverted::Meta>(),
                lookup: manager.cache::<inverted::Lookup>(),
                payload: manager.cache::<inverted::Payload>(),
                full_payload: manager.cache::<inverted::FullPayload>(),
            },
            vector_indexes: manager.cache::<vector::Index>(),
            column_data: manager.cache::<column::Data>(),
            column_arrays: manager.cache::<column::Array>(),
        })
    }

    /// Registers the process-wide instance. Called once, right after the cache manager is set up.
    pub fn init(manager: &CacheManager) {
        GlobalInstance::set(Self::new(manager));
    }

    /// Lookups as usual, but values read on a miss are not cached. Writes still go in.
    pub fn read_only(&self) -> Arc<Self> {
        Arc::new(FuseCaches {
            snapshots: self.snapshots.read_only(),
            segments: self.segments.read_only(),
            index_metas: self.index_metas.read_only(),
            bloom_filters: self.bloom_filters.read_only(),
            spatial_files: self.spatial_files.read_only(),
            vector_refine_pages: self.vector_refine_pages.read_only(),
            prune_results: self.prune_results.read_only(),
            inverted: self.inverted.read_only(),
            vector_indexes: self.vector_indexes.read_only(),
            column_data: self.column_data.read_only(),
            column_arrays: self.column_arrays.read_only(),
        })
    }

    pub fn instance() -> Arc<Self> {
        GlobalInstance::get()
    }
}
