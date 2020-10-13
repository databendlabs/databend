// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

mod datasource;
mod memory;
mod rpc;

pub use self::datasource::{IDataSourceProvider, ITable};
pub use self::memory::{MemoryProvider, MemoryTable};
