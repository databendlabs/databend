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

#![allow(clippy::uninlined_format_args)]
#![deny(unused_crate_dependencies)]

mod parquet2;
mod parquet_part;
mod parquet_reader;
mod parquet_rs;
mod processors;
mod utils;

pub use parquet2::Parquet2Reader;
pub use parquet2::Parquet2Table;
pub use parquet_part::ParquetPart;
pub use parquet_part::ParquetRowGroupPart;
pub use parquet_part::ParquetSmallFilesPart;
pub use parquet_reader::BlockIterator;
pub use parquet_reader::ParquetPartData;
pub use parquet_reader::ParquetReader;
pub use parquet_rs::ParquetReader as ParquetRSReader;
pub use parquet_rs::ParquetTable;
