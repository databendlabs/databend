// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues::DataType;
use crate::error::{Error, Result};
use crate::functions::{arithmetic, AggregatorFunction, Function};

pub struct ScalarFunctionFactory;

impl ScalarFunctionFactory {
    pub fn get(name: &str, args: &[Function]) -> Result<Function> {
        match name.to_uppercase().as_str() {
            "+" => arithmetic::AddFunction::create(args),
            "-" => arithmetic::SubFunction::create(args),
            "*" => arithmetic::MulFunction::create(args),
            "/" => arithmetic::DivFunction::create(args),
            _ => Err(Error::Unsupported(format!(
                "Unsupported Scalar Function: {}",
                name
            ))),
        }
    }
}

pub struct AggregateFunctionFactory;

impl AggregateFunctionFactory {
    pub fn get(name: &str, column: Arc<Function>, data_type: &DataType) -> Result<Function> {
        AggregatorFunction::create(name, column, data_type)
    }
}
