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

//! Iceberg Hive Metastore Catalog implementation.
//!
//! To build a hive metastore with configurations
//! # Example
//!
//! ```rust, no_run
//! use std::collections::HashMap;
//!
//! use iceberg::CatalogBuilder;
//! use iceberg_catalog_hms::{
//!     HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE, HmsCatalogBuilder,
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let catalog = HmsCatalogBuilder::default()
//!         .load(
//!             "hms",
//!             HashMap::from([
//!                 (HMS_CATALOG_PROP_URI.to_string(), "127.0.0.1:1".to_string()),
//!                 (
//!                     HMS_CATALOG_PROP_WAREHOUSE.to_string(),
//!                     "s3://warehouse".to_string(),
//!                 ),
//!             ]),
//!         )
//!         .await
//!         .unwrap();
//! }
//! ```

#![deny(missing_docs)]

mod catalog;
pub use catalog::*;

mod error;
mod schema;
mod utils;
