// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

mod tests;

mod database;
mod datasource;
mod local;
mod remote;
mod system;
mod table;
mod table_function;

pub use database::IDatabase;
pub use datasource::DataSource;
pub use datasource::IDataSource;
pub use table::ITable;
pub use table_function::ITableFunction;
