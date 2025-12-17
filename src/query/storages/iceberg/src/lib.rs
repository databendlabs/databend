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

#![feature(impl_trait_in_assoc_type)]
#![allow(clippy::collapsible_if, clippy::diverging_sub_expression)]

pub(crate) mod cache;
mod catalog;
mod database;
mod iceberg_inspect;
mod partition;
mod predicate;
mod statistics;
pub mod table;

pub use catalog::ICEBERG_CATALOG;
pub use catalog::IcebergMutableCatalog;
pub use catalog::IcebergMutableCreator;
pub use iceberg_inspect::IcebergInspectTable;
pub use table::IcebergTable;
