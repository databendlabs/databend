// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

mod tests;

mod csv;
mod datasource;
mod memory;
mod partition;
mod table;

pub use self::csv::{CsvDataSource, CsvTable};
pub use self::datasource::{get_datasource, DataSource, IDataSource};
pub use self::memory::{MemoryDataSource, MemoryTable};
pub use self::partition::{Partition, Partitions};
pub use self::table::ITable;
