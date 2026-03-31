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

//! Iceberg sql catalog implementation.
//!
//! To build a sql catalog with configurations
//! # Example
//!
//! ```rust, no_run
//! use std::collections::HashMap;
//!
//! use iceberg::CatalogBuilder;
//! use iceberg_catalog_sql::{
//!     SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE,
//!     SqlBindStyle, SqlCatalogBuilder,
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let catalog = SqlCatalogBuilder::default()
//!         .load(
//!             "sql",
//!             HashMap::from_iter([
//!                 (
//!                     SQL_CATALOG_PROP_URI.to_string(),
//!                     "http://localhost:8181".to_string(),
//!                 ),
//!                 (
//!                     SQL_CATALOG_PROP_WAREHOUSE.to_string(),
//!                     "s3://warehouse".to_string(),
//!                 ),
//!                 (
//!                     SQL_CATALOG_PROP_BIND_STYLE.to_string(),
//!                     SqlBindStyle::QMark.to_string(),
//!                 ),
//!             ]),
//!         )
//!         .await
//!         .unwrap();
//! }
//! ```

#![deny(missing_docs)]

mod catalog;
mod error;
pub use catalog::*;
