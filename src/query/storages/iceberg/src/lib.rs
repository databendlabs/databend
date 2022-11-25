// Copyright 2022 Datafuse Labs.
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

//! This is the Iceberg catalog support for databend.
//! Iceberg offered support for tables, that meaning the catalog and database data
//! should be managered by ourselves.
//!
//! # Note
//! The Iceberg catalog does not support standalone iceberg databases or iceberg tables,
//! Like the following:
//! ```text
//! ```
//!
//! For example, accessing a iceberg catalog on `s3://bkt/path/to/iceberg`
//! with following file tree:
//! ```text
//! /path/to/iceberg/
//! ┝-- /path/to/iceberg/db0/
//! |   ┝-- /path/to/iceberg/db0/tbl0
//! |   └-- /path/to/iceberg/db0/tbl1
//! └-- /path/to/iceberg/db1/    <- empty directory
//! ```
//!
//! with the following SQL or config:
//!
//! ```sql
//! CREATE CATALOG icb_ctl TYPE=ICEBERG CONNECTION=( URL='s3://bkt/path/to/iceberg' ... )
//! ```
//!
//! This will create such database heirarchy:
//! - catalog: icb_ctl
//!     - database: db0
//!         - table: tbl0
//!         - table: tbl1
//!     - database: db1
//!
//! Users should query tables with:
//! ```sql
//! SELECT * FROM icb_ctl.db0.tbl1;
//! ```

// the Iceberg Catalog implementation
mod catalog;
// data converters
mod converters;
// database implementation
mod database;
// table implementation
mod table;

pub use catalog::IcebergCatalog;
pub use catalog::ICEBERG_CATALOG;
