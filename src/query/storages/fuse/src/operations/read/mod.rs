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

mod fuse_rows_fetcher;
pub mod fuse_source;
mod native_data_source;
mod native_data_source_deserializer;
mod native_data_transform_reader;
mod native_rows_fetcher;
mod parquet_data_source;
mod parquet_data_source_deserializer;
mod parquet_data_transform_reader;
mod parquet_rows_fetcher;
mod partition_deserializer;
mod partition_reader;
mod partition_scan_meta;
mod runtime_filter_prunner;
mod source_reader;

mod block_partition_meta;
mod block_partition_receiver_source;
mod block_partition_source;
mod data_source_with_meta;
mod util;

pub use fuse_rows_fetcher::row_fetch_processor;
pub use fuse_source::build_fuse_parquet_source_pipeline;
pub use fuse_source::build_partition_source_pipeline;
pub use native_data_source_deserializer::NativeDeserializeDataTransform;
pub use parquet_data_source_deserializer::DeserializeDataTransform;
pub use parquet_data_source_reader::ReadParquetDataSource;
pub use partition_deserializer::PartitionDeserializer;
pub use partition_reader::PartitionReader;
pub use partition_reader::PartitionScanState;
pub use partition_scan_meta::PartitionScanMeta;
pub use source_reader::ReaderState;
pub use source_reader::SourceBlockReader;
pub use source_reader::SourceReader;
pub use util::need_reserve_block_info;
