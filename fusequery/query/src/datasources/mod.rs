// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod tests;
#[cfg(test)]
mod util_test;

mod database;
mod datasource;
mod local;
mod remote;
mod system;
mod table;
mod table_function;

pub mod util;

pub use database::IDatabase;
pub use datasource::DataSource;
pub use datasource::IDataSource;
pub use table::ITable;
pub use table_function::ITableFunction;
