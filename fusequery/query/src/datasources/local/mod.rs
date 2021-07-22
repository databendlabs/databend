// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod csv_table_test;
#[cfg(test)]
mod null_table_test;
#[cfg(test)]
mod parquet_table_test;

mod csv_table;
mod csv_table_stream;
pub(crate) mod local_database;
pub(crate) mod local_factory;
mod null_table;
mod parquet_table;

pub use csv_table::CsvTable;
pub use csv_table_stream::CsvTableStream;
pub use local_database::LocalDatabase;
pub use local_factory::LocalFactory;
pub use null_table::NullTable;
pub use parquet_table::ParquetTable;
