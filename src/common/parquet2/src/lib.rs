// Copyright [2021] [Jorge C Leitao]
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

// This code is a fork from upstream and will be removed soon, so we allow all clippy lints.
#![allow(clippy::all)]
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]
/// Unofficial implementation of parquet IO in Rust.

#[macro_use]
pub mod error;
#[cfg(feature = "bloom_filter")]
pub mod bloom_filter;
pub mod compression;
pub mod deserialize;
pub mod encoding;
pub mod indexes;
pub mod metadata;
pub mod page;
mod parquet_bridge;
pub mod read;
pub mod schema;
pub mod statistics;
pub mod types;
pub mod write;

use parquet_format_safe as thrift_format;
pub use streaming_decompression::fallible_streaming_iterator;
pub use streaming_decompression::FallibleStreamingIterator;

const HEADER_SIZE: u64 = PARQUET_MAGIC.len() as u64;
const FOOTER_SIZE: u64 = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

/// The number of bytes read at the end of the parquet file on first read
const DEFAULT_FOOTER_READ_SIZE: u64 = 64 * 1024;
