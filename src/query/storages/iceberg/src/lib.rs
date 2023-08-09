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

//! This is the Iceberg catalog support for databend.
//! Iceberg offered support for tables, that meaning the catalog and database data
//! should be managed by ourselves.
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
//! This will create such database hierarchy:
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
//! ## Flatten Catalogs
//!
//! There may also some iceberg storages barely storing tables in the root directory,
//! a `flatten` catalog option is introduced for such situations.
//!
//! For example, accessing iceberg tables barely storing in the root
//! directory of an S3 bucket named `samples`:
//! ```text
//! /
//! ┝-- /icbg_tbl_0
//! ┝-- /icbg_tbl_1
//! └-- /icbg_tbl_2
//! ```
//!
//! The level for database is ripped off! So the catalog should be created with following SQL:
//! ```sql
//! CREATE CATALOG icb_ctl TYPE=ICEBERG CONNECTION=(
//! URL='s3://samples/'
//! FLATTEN=true
//! ... -- credentials and other options
//! )
//! ```
//!
//! For the consistency of SQL, creating a flatten catalog doesn't meaning
//! users can query tables in catalog without specifying the database they use.
//!
//! Instead, a database named `default` will be automatically created, offering hierarchy below:
//! - catalog: icb_ctl
//!   - database: default
//!     - table: icbg_tbl_0
//!     - table: icbg_tbl_1
//!     - table: icbg_tbl_2
//!
//! And users should query along with this database.
//! ```sql
//! SELECT * FROM icb_ctl.default.icbg_tbl_0;
//! ```

#![feature(lazy_cell)]

mod catalog;
mod database;
mod partition;
mod stats;
mod table;
mod table_source;

pub use catalog::IcebergCatalog;
pub use catalog::IcebergCreator;
pub use catalog::ICEBERG_CATALOG;
