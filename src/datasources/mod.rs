// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

mod tests;

mod database;
mod datasource;
mod local;
mod partition;
mod table;

pub use self::database::{Database, IDatabase};
pub use self::datasource::DataSource;
pub use self::partition::{Partition, Partitions};
pub use self::table::ITable;
pub use local::{CsvTable, MemoryTable};
