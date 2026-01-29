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

mod agg_index;
mod block;
pub mod bloom;
mod inverted_index;
pub mod meta;
mod segment_reader;
mod snapshot_history_reader;
mod statistics;
mod utils;
mod vector_index;
mod virtual_column;

pub use agg_index::AggIndexReader;
pub use block::BlockReadResult;
pub use block::BlockReader;
pub use block::DataItem;
pub use block::NativeReaderExt;
pub use block::NativeSourceData;
pub use block::RowSelection;
pub use block::column_chunks_to_record_batch;
pub use bloom::BloomBlockFilterReader;
pub use inverted_index::InvertedIndexReader;
pub use meta::CompactSegmentInfoReader;
pub use meta::MetaReaders;
pub use meta::TableSnapshotReader;
pub use segment_reader::ColumnOrientedSegmentReader;
pub use segment_reader::RowOrientedSegmentReader;
pub use segment_reader::SegmentReader;
pub use segment_reader::read_column_oriented_segment;
pub use snapshot_history_reader::SnapshotHistoryReader;
pub use statistics::*;
pub use utils::build_columns_meta;
pub use vector_index::VectorIndexReader;
pub use vector_index::load_vector_index_files;
pub use vector_index::load_vector_index_meta;
pub use virtual_column::VirtualBlockReadResult;
pub use virtual_column::VirtualColumnReader;
pub use virtual_column::load_virtual_column_file_meta;
