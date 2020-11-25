// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues::{DataType, DataValueArithmeticOperator, DataValueComparisonOperator};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::{AggregatorFunction, ArithmeticFunction, ComparisonFunction, Function};

pub struct ScalarFunctionFactory;

impl ScalarFunctionFactory {
    pub fn get(name: &str, args: &[Function]) -> FuseQueryResult<Function> {
        match name.to_uppercase().as_str() {
            "+" => ArithmeticFunction::create(DataValueArithmeticOperator::Add, args),
            "-" => ArithmeticFunction::create(DataValueArithmeticOperator::Sub, args),
            "*" => ArithmeticFunction::create(DataValueArithmeticOperator::Mul, args),
            "/" => ArithmeticFunction::create(DataValueArithmeticOperator::Div, args),
            "=" => ComparisonFunction::create(DataValueComparisonOperator::Eq, args),
            "<" => ComparisonFunction::create(DataValueComparisonOperator::Lt, args),
            ">" => ComparisonFunction::create(DataValueComparisonOperator::Gt, args),
            "<=" => ComparisonFunction::create(DataValueComparisonOperator::LtEq, args),
            ">=" => ComparisonFunction::create(DataValueComparisonOperator::GtEq, args),
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
