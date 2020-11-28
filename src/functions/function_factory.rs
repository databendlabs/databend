// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datavalues::{
    DataValueAggregateOperator, DataValueArithmeticOperator, DataValueComparisonOperator,
    DataValueLogicOperator,
};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::{
    AggregatorFunction, ArithmeticFunction, ComparisonFunction, Function, LogicFunction,
};

pub struct ScalarFunctionFactory;

impl ScalarFunctionFactory {
    pub fn get(name: &str, args: &[Function]) -> FuseQueryResult<Function> {
        match name.to_lowercase().as_str() {
            "+" => ArithmeticFunction::try_create(DataValueArithmeticOperator::Add, args),
            "-" => ArithmeticFunction::try_create(DataValueArithmeticOperator::Sub, args),
            "*" => ArithmeticFunction::try_create(DataValueArithmeticOperator::Mul, args),
            "/" => ArithmeticFunction::try_create(DataValueArithmeticOperator::Div, args),
            "=" => ComparisonFunction::try_create(DataValueComparisonOperator::Eq, args),
            "<" => ComparisonFunction::try_create(DataValueComparisonOperator::Lt, args),
            ">" => ComparisonFunction::try_create(DataValueComparisonOperator::Gt, args),
            "<=" => ComparisonFunction::try_create(DataValueComparisonOperator::LtEq, args),
            ">=" => ComparisonFunction::try_create(DataValueComparisonOperator::GtEq, args),
            "and" => LogicFunction::try_create(DataValueLogicOperator::And, args),
            "or" => LogicFunction::try_create(DataValueLogicOperator::Or, args),
            "count" => AggregatorFunction::try_create(DataValueAggregateOperator::Count, args),
            "min" => AggregatorFunction::try_create(DataValueAggregateOperator::Min, args),
            "max" => AggregatorFunction::try_create(DataValueAggregateOperator::Max, args),
            "sum" => AggregatorFunction::try_create(DataValueAggregateOperator::Sum, args),
            _ => Err(FuseQueryError::Internal(format!(
                "Unsupported Function: {}",
                name
            ))),
        }
    }
}
