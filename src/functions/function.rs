// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::FuseQueryResult;
use crate::functions::function_logic::LogicFunction;
use crate::functions::{
    udf::UDFFunction, AggregatorFunction, AliasFunction, ArithmeticFunction, ComparisonFunction,
    ConstantFunction, FieldFunction,
};

#[derive(Clone)]
pub enum Function {
    Alias(AliasFunction),
    Constant(ConstantFunction),
    Variable(FieldFunction),
    Arithmetic(ArithmeticFunction),
    Comparison(ComparisonFunction),
    Logic(LogicFunction),
    Aggregator(AggregatorFunction),
    UDF(UDFFunction),
}

impl Function {
    pub fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        match self {
            Function::Alias(v) => v.return_type(input_schema),
            Function::Constant(v) => v.return_type(input_schema),
            Function::Variable(v) => v.return_type(input_schema),
            Function::Arithmetic(v) => v.return_type(input_schema),
            Function::Comparison(v) => v.return_type(input_schema),
            Function::Logic(v) => v.return_type(input_schema),
            Function::Aggregator(v) => v.return_type(input_schema),
            Function::UDF(v) => v.return_type(input_schema),
        }
    }

    pub fn nullable(&self, input_schema: &DataSchema) -> FuseQueryResult<bool> {
        match self {
            Function::Alias(v) => v.nullable(input_schema),
            Function::Constant(v) => v.nullable(input_schema),
            Function::Variable(v) => v.nullable(input_schema),
            Function::Arithmetic(v) => v.nullable(input_schema),
            Function::Comparison(v) => v.nullable(input_schema),
            Function::Logic(v) => v.nullable(input_schema),
            Function::Aggregator(v) => v.nullable(input_schema),
            Function::UDF(v) => v.nullable(input_schema),
        }
    }

    pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        match self {
            Function::Alias(v) => v.eval(block),
            Function::Constant(v) => v.eval(block),
            Function::Variable(v) => v.eval(block),
            Function::Arithmetic(v) => v.eval(block),
            Function::Comparison(v) => v.eval(block),
            Function::Logic(v) => v.eval(block),
            Function::Aggregator(v) => v.eval(block),
            Function::UDF(v) => v.eval(block),
        }
    }

    pub fn set_depth(&mut self, depth: usize) {
        match self {
            Function::Alias(v) => v.set_depth(depth),
            Function::Constant(v) => v.set_depth(depth),
            Function::Variable(v) => v.set_depth(depth),
            Function::Arithmetic(v) => v.set_depth(depth),
            Function::Comparison(v) => v.set_depth(depth),
            Function::Logic(v) => v.set_depth(depth),
            Function::Aggregator(v) => v.set_depth(depth),
            Function::UDF(v) => v.set_depth(depth),
        }
    }

    // Accumulator all the block to one state.
    // This is used in aggregation.
    // sum(state) = sum(block1) + sum(block2) ...
    pub fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        match self {
            Function::Alias(v) => v.accumulate(block),
            Function::Constant(v) => v.accumulate(block),
            Function::Variable(v) => v.accumulate(block),
            Function::Arithmetic(v) => v.accumulate(block),
            Function::Comparison(v) => v.accumulate(block),
            Function::Logic(v) => v.accumulate(block),
            Function::Aggregator(v) => v.accumulate(block),
            Function::UDF(v) => v.accumulate(block),
        }
    }

    // Get the final state for all the accumulator.
    pub fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        match self {
            Function::Alias(v) => v.accumulate_result(),
            Function::Constant(v) => v.accumulate_result(),
            Function::Variable(v) => v.accumulate_result(),
            Function::Arithmetic(v) => v.accumulate_result(),
            Function::Comparison(v) => v.accumulate_result(),
            Function::Logic(v) => v.accumulate_result(),
            Function::Aggregator(v) => v.accumulate_result(),
            Function::UDF(v) => v.accumulate_result(),
        }
    }

    // Merge partial accumulator results(state) to one.
    // merge(state) = sum(state1) + sum(state2)
    // This is used in aggregation.
    pub fn merge(&mut self, states: &[DataValue]) -> FuseQueryResult<()> {
        match self {
            Function::Alias(v) => v.merge(states),
            Function::Constant(v) => v.merge(states),
            Function::Variable(v) => v.merge(states),
            Function::Arithmetic(v) => v.merge(states),
            Function::Comparison(v) => v.merge(states),
            Function::Logic(v) => v.merge(states),
            Function::Aggregator(v) => v.merge(states),
            Function::UDF(v) => v.merge(states),
        }
    }

    // Return the final result merge(state)
    // This is used in aggregation.
    pub fn merge_result(&self) -> FuseQueryResult<DataValue> {
        match self {
            Function::Alias(v) => v.merge_result(),
            Function::Constant(v) => v.merge_result(),
            Function::Variable(v) => v.merge_result(),
            Function::Arithmetic(v) => v.merge_result(),
            Function::Comparison(v) => v.merge_result(),
            Function::Logic(v) => v.merge_result(),
            Function::Aggregator(v) => v.merge_result(),
            Function::UDF(v) => v.merge_result(),
        }
    }
}

impl fmt::Debug for Function {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Function::Alias(v) => write!(f, "{}", v),
            Function::Constant(v) => write!(f, "{}", v),
            Function::Variable(v) => write!(f, "{}", v),
            Function::Arithmetic(v) => write!(f, "{}", v),
            Function::Comparison(v) => write!(f, "{}", v),
            Function::Logic(v) => write!(f, "{}", v),
            Function::Aggregator(v) => write!(f, "{}", v),
            Function::UDF(v) => write!(f, "{}", v),
        }
    }
}
