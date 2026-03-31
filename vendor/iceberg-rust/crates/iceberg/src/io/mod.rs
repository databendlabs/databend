// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! File io implementation.
//!
//! # How to build `FileIO`
//!
//! We provided a `FileIOBuilder` to build `FileIO` from scratch. For example:
//!
//! ```rust
//! use iceberg::Result;
//! use iceberg::io::{FileIOBuilder, S3_REGION};
//!
//! # fn test() -> Result<()> {
//! // Build a memory file io.
//! let file_io = FileIOBuilder::new("memory").build()?;
//! // Build an fs file io.
//! let file_io = FileIOBuilder::new("fs").build()?;
//! // Build an s3 file io.
//! let file_io = FileIOBuilder::new("s3")
//!     .with_prop(S3_REGION, "us-east-1")
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! Or you can pass a path to ask `FileIO` to infer schema for you:
//!
//! ```rust
//! use iceberg::Result;
//! use iceberg::io::{FileIO, S3_REGION};
//!
//! # fn test() -> Result<()> {
//! // Build a memory file io.
//! let file_io = FileIO::from_path("memory:///")?.build()?;
//! // Build an fs file io.
//! let file_io = FileIO::from_path("fs:///tmp")?.build()?;
//! // Build an s3 file io.
//! let file_io = FileIO::from_path("s3://bucket/a")?
//!     .with_prop(S3_REGION, "us-east-1")
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! # How to use `FileIO`
//!
//! Currently `FileIO` provides simple methods for file operations:
//!
//! - `delete`: Delete file.
//! - `exists`: Check if file exists.
//! - `new_input`: Create input file for reading.
//! - `new_output`: Create output file for writing.

mod file_io;
mod storage;

pub use file_io::*;
pub(crate) mod object_cache;

#[cfg(feature = "storage-azdls")]
mod storage_azdls;
#[cfg(feature = "storage-fs")]
mod storage_fs;
#[cfg(feature = "storage-gcs")]
mod storage_gcs;
#[cfg(feature = "storage-memory")]
mod storage_memory;
#[cfg(feature = "storage-oss")]
mod storage_oss;
#[cfg(feature = "storage-s3")]
mod storage_s3;

#[cfg(feature = "storage-azdls")]
pub use storage_azdls::*;
#[cfg(feature = "storage-fs")]
use storage_fs::*;
#[cfg(feature = "storage-gcs")]
pub use storage_gcs::*;
#[cfg(feature = "storage-memory")]
use storage_memory::*;
#[cfg(feature = "storage-oss")]
pub use storage_oss::*;
#[cfg(feature = "storage-s3")]
pub use storage_s3::*;

pub(crate) fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}
