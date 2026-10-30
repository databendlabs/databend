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

use crate::pool_caps::KindLimit;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Pool {
    Meta,
    Index,
    Data,
}

impl Pool {
    pub const ALL: [Pool; 3] = [Pool::Meta, Pool::Index, Pool::Data];

    /// Order in which memory pressure reclaims pools.
    pub const RECLAIM_ORDER: [Pool; 3] = [Pool::Data, Pool::Index, Pool::Meta];

    pub const fn index(self) -> usize {
        self as usize
    }

    pub const fn name(self) -> &'static str {
        match self {
            Pool::Meta => "meta",
            Pool::Index => "index",
            Pool::Data => "data",
        }
    }

    pub fn from_name(name: &str) -> Option<Pool> {
        Pool::ALL.into_iter().find(|p| p.name() == name)
    }
}

macro_rules! cache_kinds {
    ($($kind:ident => ($pool:ident, $name:literal, $legacy:literal)),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[repr(u8)]
        pub enum CacheKind {
            $($kind),+
        }

        impl CacheKind {
            pub const ALL: &'static [CacheKind] = &[$(CacheKind::$kind),+];

            pub const fn pool(self) -> Pool {
                match self {
                    $(CacheKind::$kind => Pool::$pool),+
                }
            }

            pub const fn name(self) -> &'static str {
                match self {
                    $(CacheKind::$kind => $name),+
                }
            }

            pub const fn legacy_name(self) -> &'static str {
                match self {
                    $(CacheKind::$kind => $legacy),+
                }
            }
        }
    };
}

cache_kinds! {
    TableSnapshot => (Meta, "table_snapshot", "memory_cache_table_snapshot"),
    TableSnapshotStats => (Meta, "table_snapshot_stats", "memory_cache_table_statistics"),
    SegmentInfo => (Meta, "segment_info", "memory_cache_compact_segment_info"),
    ColumnOrientedSegment => (Meta, "column_oriented_segment", "memory_cache_column_oriented_segment_info"),
    SegmentBlockMetas => (Meta, "segment_block_metas", "memory_cache_segment_block_metas"),
    SegmentStats => (Meta, "segment_stats", "memory_cache_segment_statistics"),
    BloomIndexMeta => (Meta, "bloom_index_meta", "memory_cache_bloom_index_file_meta_data"),
    InvertedIndexMeta => (Meta, "inverted_index_meta", "memory_cache_inverted_index_meta_data"),
    VectorIndexMeta => (Meta, "vector_index_meta", "memory_cache_vector_index_file_meta_data"),
    SpatialIndexMeta => (Meta, "spatial_index_meta", "memory_cache_spatial_index_file_meta_data"),
    VirtualColumnMeta => (Meta, "virtual_column_meta", "memory_cache_virtual_column_file_meta_data"),
    ParquetMeta => (Meta, "parquet_meta", "memory_cache_parquet_meta_data"),
    IcebergTable => (Meta, "iceberg_table", "memory_cache_iceberg_table"),
    PruneResult => (Meta, "prune_result", "memory_cache_prune_partitions"),
    BloomFilter => (Index, "bloom_filter", "memory_cache_bloom_index_filter"),
    VectorIndex => (Index, "vector_index", "memory_cache_vector_index_file"),
    VectorRefine => (Index, "vector_refine", "memory_cache_vector_index_refine"),
    InvertedIndexLookup => (Index, "inverted_index_lookup", "memory_cache_inverted_index_lookup"),
    InvertedIndexPayload => (Index, "inverted_index_payload", "memory_cache_inverted_index_payload"),
    InvertedIndexFullPayload => (Index, "inverted_index_full_payload", "memory_cache_inverted_index_full_payload"),
    SpatialIndex => (Index, "spatial_index", "memory_cache_spatial_index_file"),
    ColumnData => (Data, "column_data", "memory_cache_column_data"),
    ColumnArray => (Data, "column_array", "memory_cache_table_data"),
}

impl CacheKind {
    pub const fn index(self) -> usize {
        self as usize
    }

    pub const fn admit_after(self) -> u8 {
        match self.pool() {
            Pool::Data => 2,
            Pool::Meta | Pool::Index => 1,
        }
    }

    pub const fn default_limit(self) -> KindLimit {
        match self {
            CacheKind::ColumnData | CacheKind::ColumnArray => KindLimit::DISABLED,
            _ => KindLimit::UNLIMITED,
        }
    }

    /// Accepts the kind name, the legacy memory cache name, and the legacy hybrid cache name
    /// (the memory name without its `memory_` prefix, e.g. `cache_bloom_index_filter`).
    pub fn from_any_name(name: &str) -> Option<CacheKind> {
        CacheKind::ALL.iter().copied().find(|k| {
            k.name() == name
                || k.legacy_name() == name
                || k.legacy_name().strip_prefix("memory_") == Some(name)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kinds_are_indexed_densely() {
        assert_eq!(CacheKind::ALL.len(), 23);
        for (i, kind) in CacheKind::ALL.iter().enumerate() {
            assert_eq!(kind.index(), i);
        }
    }

    #[test]
    fn test_names_are_unique_and_resolvable() {
        for kind in CacheKind::ALL {
            assert_eq!(CacheKind::from_any_name(kind.name()), Some(*kind));
            assert_eq!(CacheKind::from_any_name(kind.legacy_name()), Some(*kind));
        }
        assert_eq!(
            CacheKind::from_any_name("cache_bloom_index_filter"),
            Some(CacheKind::BloomFilter)
        );
        assert_eq!(CacheKind::from_any_name("unknown"), None);
    }
}
