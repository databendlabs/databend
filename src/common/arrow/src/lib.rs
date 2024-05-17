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

#![feature(iter_advance_by)]
#![allow(clippy::unconditional_recursion)]
#![cfg_attr(feature = "simd", feature(portable_simd))]
#![allow(clippy::redundant_closure_call)]
#![allow(clippy::non_canonical_partial_ord_impl)]
#![allow(dead_code)]

//#[macro_use]
// mod errors;

pub mod arrow;
pub mod native;
mod parquet_read;
mod parquet_write;
pub mod schema_projection;

pub use arrow_format;
pub use parquet2 as parquet;
pub use parquet_read::read_columns_async;
pub use parquet_read::read_columns_many_async;
pub use parquet_write::write_parquet_file;

pub type ArrayRef = Box<dyn crate::arrow::array::Array>;
