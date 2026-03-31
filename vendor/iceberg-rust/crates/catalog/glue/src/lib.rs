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

//! Iceberg Glue Catalog implementation.
//!
//! To build a glue catalog with configurations
//! # Example
//!
//! ```rust, no_run
//! use std::collections::HashMap;
//!
//! use iceberg::CatalogBuilder;
//! use iceberg_catalog_glue::{GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalogBuilder};
//!
//! #[tokio::main]
//! async fn main() {
//!     let catalog = GlueCatalogBuilder::default()
//!         .load(
//!             "glue",
//!             HashMap::from([(
//!                 GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
//!                 "s3://warehouse".to_string(),
//!             )]),
//!         )
//!         .await
//!         .unwrap();
//! }
//! ```

#![deny(missing_docs)]

mod catalog;
mod error;
mod schema;
mod utils;
pub use catalog::*;
pub use utils::{
    AWS_ACCESS_KEY_ID, AWS_PROFILE_NAME, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
};
