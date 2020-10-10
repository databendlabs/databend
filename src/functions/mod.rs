// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

mod tests;

pub mod aggregate;
pub mod arithmetic;
pub mod function;
pub mod function_constant;
pub mod function_factory;
pub mod function_variable;

use self::function::Function;
use self::function_constant::ConstantFunction;
pub use self::function_factory::FunctionFactory;
use self::function_variable::VariableFunction;

///
/// crates.
use crate::datablocks::data_block::DataBlock;
use crate::datatypes::{DataArrayRef, DataSchema, DataType, DataValue};
use crate::error::{Error, Result};
