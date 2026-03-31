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

//! Apache Iceberg Official Native Rust Implementation
//!
//! # Examples
//!
//! ## Scan A Table
//!
//! ```rust, no_run
//! use std::collections::HashMap;
//!
//! use futures::TryStreamExt;
//! use iceberg::io::{FileIO, FileIOBuilder};
//! use iceberg::memory::MemoryCatalogBuilder;
//! use iceberg::{Catalog, CatalogBuilder, MemoryCatalog, Result, TableIdent};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Connect to a catalog.
//!     use iceberg::memory::MEMORY_CATALOG_WAREHOUSE;
//!     let catalog = MemoryCatalogBuilder::default()
//!         .load(
//!             "memory",
//!             HashMap::from([(
//!                 MEMORY_CATALOG_WAREHOUSE.to_string(),
//!                 "file:///path/to/warehouse".to_string(),
//!             )]),
//!         )
//!         .await?;
//!     // Load table from catalog.
//!     let table = catalog
//!         .load_table(&TableIdent::from_strs(["hello", "world"])?)
//!         .await?;
//!     // Build table scan.
//!     let stream = table
//!         .scan()
//!         .select(["name", "id"])
//!         .build()?
//!         .to_arrow()
//!         .await?;
//!
//!     // Consume this stream like arrow record batch stream.
//!     let _data: Vec<_> = stream.try_collect().await?;
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]

#[macro_use]
extern crate derive_builder;
extern crate core;

mod error;
pub use error::{Error, ErrorKind, Result};

mod catalog;

pub use catalog::*;

pub mod table;

mod avro;
pub mod cache;
pub mod io;
pub mod spec;

pub mod inspect;
pub mod scan;

pub mod expr;
pub mod transaction;
pub mod transform;

mod runtime;

pub mod arrow;
pub(crate) mod delete_file_index;
pub mod test_utils;
mod utils;
pub mod writer;

mod delete_vector;
pub mod metadata_columns;
pub mod puffin;
