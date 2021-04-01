// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

mod tests;

mod datasource;
mod local;
mod remote;
mod system;
mod table;
mod table_factory;
mod table_function;

pub use datasource::{DataSource, IDataSource};
pub use table::ITable;
pub use table_factory::TableFactory;
pub use table_function::ITableFunction;
