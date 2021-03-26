// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

mod tests;

mod datasource;
mod local;
mod remote;
mod system;
mod table;
mod table_factory;

pub use datasource::{DataSource, IDataSource};
pub use table::ITable;
pub use table_factory::TableFactory;
