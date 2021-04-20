// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod clusters_table_test;
#[cfg(test)]
mod functions_table_test;
#[cfg(test)]
mod numbers_table_test;
#[cfg(test)]
mod settings_table_test;
#[cfg(test)]
mod tables_table_test;
#[cfg(test)]
mod databases_table_test;

mod clusters_table;
mod functions_table;
mod numbers_stream;
mod numbers_table;
mod one_table;
mod settings_table;
mod system_database;
mod system_factory;
mod tables_table;
mod databases_table;

pub use clusters_table::ClustersTable;
pub use functions_table::FunctionsTable;
pub use numbers_stream::NumbersStream;
pub use numbers_table::NumbersTable;
pub use one_table::OneTable;
pub use settings_table::SettingsTable;
pub use system_database::SystemDatabase;
pub use system_factory::SystemFactory;
pub use tables_table::TablesTable;
pub use databases_table::DatabasesTable;
