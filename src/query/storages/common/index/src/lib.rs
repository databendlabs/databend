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

#![allow(
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::manual_is_multiple_of,
    clippy::needless_range_loop,
    clippy::uninlined_format_args
)]
#![allow(incomplete_features)]
#![feature(deref_patterns)]

mod bloom_index;
mod eliminate_cast;
pub mod filters;
mod granule_index;
mod hnsw_index;
mod index;
mod index_common;
mod inverted_index;
mod kmeans;
mod range_index;
mod row_mapping;
mod spatial_index;
mod spatial_predicate;
mod statistics_cast;
mod vector;
mod vector_bundle;
mod virtual_column;

pub use bloom_index::BloomIndex;
pub use bloom_index::BloomIndexBuilder;
pub use bloom_index::BloomIndexMeta;
pub use bloom_index::BloomIndexResult;
pub use bloom_index::BloomIndexType;
pub use bloom_index::DEFAULT_NGRAM_FALSE_POSITIVE_RATE;
pub use bloom_index::FilterEvalResult;
pub use bloom_index::NgramArgs;
pub use bloom_index::NgramHashAlgorithm;
pub use bloom_index::NgramLikeScalarMap;
pub use eliminate_cast::eliminate_cast;
pub use granule_index::GranuleIndex;
pub use hnsw_index::DistanceType;
pub use hnsw_index::FixedLengthPriorityQueue;
pub use hnsw_index::HNSWIndex;
pub use hnsw_index::ScoredPointOffset;
pub use hnsw_index::VectorIndexFile;
pub use hnsw_index::hnsw_component_names;
pub use index::Index;
pub use index_common::IndexFile;
pub use index_common::IndexMeta;
pub use inverted_index::BundleExternalFiles;
pub use inverted_index::BundleFileRanges;
pub use inverted_index::BundleOpenSlice;
pub use inverted_index::BundleSizes;
pub use inverted_index::ExternalFile;
pub use inverted_index::FieldCostTable;
pub use inverted_index::FooterDirectory;
pub use inverted_index::INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE;
pub use inverted_index::INVERTED_INDEX_BUNDLE_MAX_FOOTER_SIZE;
pub use inverted_index::INVERTED_INDEX_BUNDLE_OBJECT_SUFFIX;
pub use inverted_index::INVERTED_INDEX_BUNDLE_TRAILER_LEN;
pub use inverted_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
pub use inverted_index::INVERTED_INDEX_STREAM_THRESHOLD;
pub use inverted_index::InvertedIndexBundleBuilder;
pub use inverted_index::InvertedIndexBundleFooter;
pub use inverted_index::InvertedIndexBundleVersion;
pub use inverted_index::InvertedIndexLookupBytes;
pub use inverted_index::InvertedIndexMeta;
pub use inverted_index::InvertedIndexOutputDirectory;
pub use inverted_index::InvertedIndexPayloadBytes;
pub use inverted_index::MANAGED_JSON_PATH;
pub use inverted_index::META_JSON_PATH;
pub use inverted_index::MergeInterruptCheck;
pub use inverted_index::MergeOptions;
pub use inverted_index::MergeOutput;
pub use inverted_index::MergeSource;
pub use inverted_index::MergeSourceDirectory;
pub use inverted_index::MergeSourceReader;
pub use inverted_index::OutputSlice;
pub use inverted_index::OutputWriteContext;
pub use inverted_index::OutputWriter;
pub use inverted_index::SOURCE_OPEN_TAIL_READ_SIZE;
pub use inverted_index::STREAMING_CHUNK_SIZE;
pub use inverted_index::SearchPinDirectory;
pub use inverted_index::SliceDoc;
pub use inverted_index::SliceTerm;
pub use inverted_index::StreamingFileHandle;
pub use inverted_index::StreamingReadStats;
pub use inverted_index::TERM_COST_CHECKPOINT_BYTES;
pub use inverted_index::TERM_WINDOW_BYTES;
pub use inverted_index::TermCostCheckpoint;
pub use inverted_index::TermWindow;
pub use inverted_index::check_schema;
pub use inverted_index::checkpoint_bytes_for;
pub use inverted_index::collect_index_open_slices;
pub use inverted_index::indexed_fields;
pub use inverted_index::inverted_index_meta_cache_key;
pub use inverted_index::json_term_record_option;
pub use inverted_index::merge_in_process;
pub use inverted_index::merge_window;
pub use inverted_index::open_merge_source;
pub use inverted_index::plan_field_windows;
pub use inverted_index::plan_index_windows;
pub use inverted_index::scan_field_costs;
pub use inverted_index::scan_source_costs;
pub use kmeans::KMeans;
pub use kmeans::KMeansResult;
pub use range_index::RangeIndex;
pub use range_index::statistics_to_domain;
pub use row_mapping::RowMapping;
pub use row_mapping::SourceRows;
pub use spatial_index::SpatialIndexFile;
pub use spatial_index::SpatialIndexMeta;
pub use spatial_index::rect_contains;
pub use spatial_index::rects_distance_intersect;
pub use spatial_index::rects_intersect;
pub use spatial_index::spatial_false_domain;
pub use spatial_predicate::SpatialPredicate;
pub use spatial_predicate::SpatialPredicateOp;
pub use spatial_predicate::SpatialPredicateResult;
pub use spatial_predicate::collect_spatial_predicates;
pub use statistics_cast::cast_virtual_column_statistics;
pub use vector::Fp16QueryScorer;
pub use vector::normalize_vector;
pub use vector::vector_stat_distance;
pub use vector_bundle::*;
pub use virtual_column::VIRTUAL_COLUMN_NODES_KEY;
pub use virtual_column::VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY;
pub use virtual_column::VIRTUAL_COLUMN_STRING_TABLE_JSON_KEY;
pub use virtual_column::VIRTUAL_COLUMN_STRING_TABLE_KEY;
pub use virtual_column::VirtualColumnFileMeta;
pub use virtual_column::VirtualColumnIdWithMeta;
pub use virtual_column::VirtualColumnNameIndex;
pub use virtual_column::VirtualColumnNode;
pub use virtual_column::VirtualColumnSharedColumnIdMap;
pub use virtual_column::VirtualColumnSharedColumnIds;
pub use virtual_column::VirtualColumnSharedColumnMetaMap;
pub use virtual_column::VirtualColumnSharedColumnMetas;
pub use virtual_column::VirtualColumnSharedDataType;
pub use virtual_column::VirtualColumnStat;
pub use virtual_column::VirtualColumnStatsOfNames;
pub use virtual_column::encode_compact_virtual_column_nodes;
pub use virtual_column::encode_compact_virtual_column_shared_ids;
pub use virtual_column::encode_compact_virtual_column_string_table;

#[cfg(test)]
pub(crate) fn init_test_runtime() {
    use std::sync::Once;

    use databend_common_base::base::GlobalInstance;
    use databend_common_base::runtime::GlobalIORuntime;

    static INIT: Once = Once::new();
    INIT.call_once(|| {
        GlobalInstance::init_production();
        GlobalIORuntime::init(2).unwrap();
    });
}
