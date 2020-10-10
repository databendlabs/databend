// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

mod tests;

mod aggregate_count;

pub use self::aggregate_count::CountAggregateFunction;

use crate::datablocks::data_block::DataBlock;
use crate::datatypes::{DataArrayRef, DataSchema, DataType, DataValue};
use crate::error::Result;
use crate::functions::Function;
