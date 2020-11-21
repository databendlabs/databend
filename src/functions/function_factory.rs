// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues::DataType;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::{arithmetics, AggregatorFunction, Function};

pub struct ScalarFunctionFactory;

impl ScalarFunctionFactory {
    pub fn get(name: &str, args: &[Function]) -> FuseQueryResult<Function> {
        match name.to_uppercase().as_str() {
            "+" => arithmetics::AddFunction::create(args),
            "-" => arithmetics::SubFunction::create(args),
            "*" => arithmetics::MulFunction::create(args),
            "/" => arithmetics::DivFunction::create(args),
            _ => Err(FuseQueryError::Unsupported(format!(
                "Unsupported Scalar Function: {}",
                name
            ))),
        }
    }
}

pub struct AggregateFunctionFactory;

impl AggregateFunctionFactory {
    pub fn get(
        name: &str,
        column: Arc<Function>,
        data_type: &DataType,
    ) -> FuseQueryResult<Function> {
        AggregatorFunction::create(name, column, data_type)
    }
}
