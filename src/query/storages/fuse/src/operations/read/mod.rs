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

mod block_format;
mod fuse_rows_fetcher;
pub mod fuse_source;
mod native_data_source;
mod native_data_source_deserializer;
mod parquet_data_source;
mod parquet_data_source_deserializer;
mod parquet_rows_fetcher;
mod raw_data_source;
mod read_block_context;
mod read_data_source;
mod read_data_transform;
mod read_state;

mod block_partition_meta;
mod data_source_with_meta;
mod partition_stream;
mod util;

pub use fuse_rows_fetcher::row_fetch_processor;
pub use fuse_source::build_fuse_source_pipeline;
pub use native_data_source_deserializer::NativeDeserializeDataTransform;
pub use parquet_data_source_deserializer::DeserializeDataTransform;
pub use read_state::ReadState;
pub use util::need_reserve_block_info;
