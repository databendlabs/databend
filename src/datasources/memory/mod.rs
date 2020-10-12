// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

mod memory_provider;
mod memory_table;

pub use self::memory_provider::MemoryProvider;
pub use self::memory_table::MemoryTable;

use crate::datablocks::DataBlock;
use crate::datasources::{IDataSourceProvider, ITable};
use crate::datatypes::DataSchemaRef;
use crate::error::{Error, Result};
use crate::planners::{PlanNode, ReadDataSourcePlan};
