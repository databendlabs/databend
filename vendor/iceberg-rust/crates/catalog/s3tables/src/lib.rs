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

//! Iceberg s3tables catalog implementation.
//!
//! To build an s3tables catalog with configurations
//!
//! # Example
//!
//! ```rust, no_run
//! use std::collections::HashMap;
//!
//! use iceberg::CatalogBuilder;
//! use iceberg_catalog_s3tables::{
//!     S3TABLES_CATALOG_PROP_ENDPOINT_URL, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN,
//!     S3TablesCatalogBuilder,
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let catalog = S3TablesCatalogBuilder::default()
//!         .with_endpoint_url("http://localhost:4566")
//!         .load(
//!             "s3tables",
//!             HashMap::from([(
//!                 S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
//!                 "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket".to_string(),
//!             )]),
//!         )
//!         .await
//!         .unwrap();
//! }
//! ```

#![deny(missing_docs)]

mod catalog;
mod utils;

pub use catalog::*;
