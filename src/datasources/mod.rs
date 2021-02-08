// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

mod tests;

mod datasource;
mod local;
mod partition;
mod remote;
mod statistics;
mod system;
mod table;

pub use self::datasource::{DataSource, IDataSource};
pub use self::partition::{Partition, Partitions};
pub use self::statistics::Statistics;
pub use self::table::ITable;
