// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

mod memory_source;

pub use self::memory_source::MemorySource;

use crate::datablocks::DataBlock;
use crate::datasources::IDataSourceProvider;
use crate::datatypes::DataSchemaRef;
use crate::error::Result;
use crate::planners::PlanNode;
