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
#![feature(box_patterns)]
#![feature(never_type)]

mod bloom_index;
mod eliminate_cast;
pub mod filters;
mod hnsw_index;
mod index;
mod inverted_index;
mod page_index;
mod range_index;
mod virtual_column;

pub use bloom_index::BloomIndex;
pub use bloom_index::BloomIndexBuilder;
pub use bloom_index::BloomIndexMeta;
pub use bloom_index::BloomIndexResult;
pub use bloom_index::FilterEvalResult;
pub use bloom_index::NgramArgs;
pub use eliminate_cast::eliminate_cast;
pub use hnsw_index::DistanceType;
pub use hnsw_index::FixedLengthPriorityQueue;
pub use hnsw_index::HNSWIndex;
pub use hnsw_index::ScoredPointOffset;
pub use hnsw_index::VectorIndexFile;
pub use hnsw_index::VectorIndexMeta;
pub use index::Index;
pub use inverted_index::DocIdsCollector;
pub use inverted_index::InvertedIndexDirectory;
pub use inverted_index::InvertedIndexFile;
pub use inverted_index::InvertedIndexMeta;
pub use inverted_index::TermReader;
pub use inverted_index::build_tantivy_footer;
pub use inverted_index::extract_component_fields;
pub use inverted_index::extract_fsts;
pub use page_index::PageIndex;
pub use range_index::RangeIndex;
pub use range_index::statistics_to_domain;
pub use virtual_column::VIRTUAL_COLUMN_NODES_KEY;
pub use virtual_column::VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY;
pub use virtual_column::VIRTUAL_COLUMN_STRING_TABLE_KEY;
pub use virtual_column::VirtualColumnFileMeta;
pub use virtual_column::VirtualColumnIdWithMeta;
pub use virtual_column::VirtualColumnNameIndex;
pub use virtual_column::VirtualColumnNode;
