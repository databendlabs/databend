// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub use common::Common;
pub use database::Database;
pub use table::Table;
pub use table::TablePtr;
pub use table_function::TableFunction;

pub use crate::catalogs::impls::database_catalog::DatabaseCatalog;

#[cfg(test)]
mod common_test;
#[cfg(test)]
mod tests;

mod common;
mod database;
pub(crate) mod local;
pub(crate) mod remote;
pub(crate) mod system;
mod table;
mod table_function;
