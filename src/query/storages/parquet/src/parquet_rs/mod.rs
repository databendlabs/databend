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

mod copy_into_table;
mod parquet_reader;
mod parquet_table;
mod partition;
mod pruning;
mod source;
mod statistics;

mod meta;
mod schema;

pub use copy_into_table::ParquetTableForCopy;
pub use meta::read_metas_in_parallel_for_copy;
pub use meta::read_parquet_metas_batch;
pub use parquet_reader::InMemoryRowGroup;
pub use parquet_reader::ParquetFileReader;
pub use parquet_reader::ParquetRSFullReader;
pub use parquet_reader::ParquetRSReaderBuilder;
pub use parquet_reader::ParquetRSRowGroupReader;
pub use parquet_table::ParquetRSTable;
pub use partition::ParquetRSRowGroupPart;
pub use pruning::ParquetRSPruner;
